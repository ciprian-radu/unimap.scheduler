package ro.ulbsibiu.acaps.scheduler;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import ro.ulbsibiu.acaps.ctg.xml.apcg.ApcgType;
import ro.ulbsibiu.acaps.ctg.xml.apcg.ObjectFactory;
import ro.ulbsibiu.acaps.ctg.xml.core.CoreType;
import ro.ulbsibiu.acaps.ctg.xml.task.TaskType;

/**
 * This @link{Scheduler} assigns tasks to available cores in a random fashion.
 * 
 * @author cipi
 * 
 */
public class RandomScheduler implements Scheduler {

	/** the ID of the Communication Task Graph */
	private String ctgId;

	/** the XML files containing the tasks */
	private File[] taskXmls;

	/** the XML files containing the cores */
	private File[] coreXmls;

	/** each task is mapped to a core */
	private Map<File, File> tasksToCores;

	/**
	 * Constructor
	 * 
	 * @param ctgId
	 *            the ID of the Communication Task Graph (cannot be empty)
	 * @param tasksFilePath
	 *            the XML files containing the tasks (cannot be empty)
	 * @param coresFilePath
	 *            the XML files containing the cores (cannot be empty)
	 */
	public RandomScheduler(String ctgId, String tasksFilePath,
			String coresFilePath) {
		assert ctgId != null && ctgId.length() > 0;
		assert tasksFilePath != null && tasksFilePath.length() > 0;
		assert coresFilePath != null && coresFilePath.length() > 0;

		this.ctgId = ctgId;

		File tasksFile = new File(tasksFilePath);
		assert tasksFile.isDirectory();

		File coresFile = new File(coresFilePath);
		assert coresFile.isDirectory();

		taskXmls = tasksFile.listFiles(new FilenameFilter() {

			@Override
			public boolean accept(File file, String name) {
				return name.endsWith(".xml");
			}
		});

		coreXmls = coresFile.listFiles(new FilenameFilter() {

			@Override
			public boolean accept(File file, String name) {
				return name.endsWith(".xml");
			}
		});

		tasksToCores = null;
	}

	/**
	 * Schedules the CTG tasks to the available cores in a random fashion.
	 * 
	 * @see ApcgType
	 * 
	 * @return a String containing the APCG XML
	 */
	@Override
	public String schedule() {
		tasksToCores = new HashMap<File, File>(taskXmls.length);
		Random random = new Random();
		for (int i = 0; i < taskXmls.length; i++) {
			tasksToCores.put(taskXmls[i],
					coreXmls[random.nextInt(coreXmls.length)]);
		}
		String apcgXml = null;
		try {
			apcgXml = generateApcg();
		} catch (JAXBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return apcgXml;
	}

	private String getCoreId(File file) throws JAXBException {
		JAXBContext jaxbContext = JAXBContext.newInstance("ro.ulbsibiu.acaps.ctg.xml.core");
		Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
		JAXBElement<CoreType> coreXml = (JAXBElement<CoreType>) unmarshaller.unmarshal(file);
		return coreXml.getValue().getID();
	}
	
	private String getTaskId(File file) throws JAXBException {
		JAXBContext jaxbContext = JAXBContext.newInstance("ro.ulbsibiu.acaps.ctg.xml.task");
		Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
		JAXBElement<TaskType> taskXml = (JAXBElement<TaskType>) unmarshaller.unmarshal(file);
		return taskXml.getValue().getID();
	}
	
	private String generateApcg() throws JAXBException {
		assert tasksToCores != null;

		ObjectFactory apcgFactory = new ObjectFactory();
		ApcgType apcgType = new ApcgType();
		apcgType.setId(ctgId);
		apcgType.setCtg(ctgId);
		
		Map<File, Set<File>> coreToTasks = new HashMap<File, Set<File>>();
		Set<File> tasks = tasksToCores.keySet();
		for (File task : tasks) {
			File core = tasksToCores.get(task);
			Set<File> set = coreToTasks.get(core);
			if (set == null) {
				set = new LinkedHashSet<File>();
			}
			set.add(task);
			coreToTasks.put(core, set);
		}
		
		Set<File> cores = coreToTasks.keySet();
		for (File core : cores) {
			String coreId = getCoreId(core);
			Set<File> set = coreToTasks.get(core);
			ro.ulbsibiu.acaps.ctg.xml.apcg.CoreType coreType = new ro.ulbsibiu.acaps.ctg.xml.apcg.CoreType();
			coreType.setId(coreId);
			for (File task : set) {
				String taskId = getTaskId(task);
				coreType.getTask().add(taskId);
			}
			apcgType.getCore().add(coreType);
		}
		
		JAXBContext jaxbContext = JAXBContext.newInstance(ApcgType.class);
		Marshaller marshaller = jaxbContext.createMarshaller();
		StringWriter stringWriter = new StringWriter();
		JAXBElement<ApcgType> apcg = apcgFactory.createApcg(apcgType);
		marshaller.marshal(apcg, stringWriter);
		
		return stringWriter.toString();
	}

	public static void main(String[] args) throws FileNotFoundException {
		String path = "xml" + File.separator + "e3s" + File.separator
				+ "auto-indust-mocsyn.tgff" + File.separator;
		Scheduler scheduler = new RandomScheduler("0", path + "ctg-0"
				+ File.separator + "tasks", path + "cores");
		String apcgXml = scheduler.schedule();
		PrintWriter pw = new PrintWriter(path + "ctg-0"
				+ File.separator + "apcg-0.xml");
		pw.write(apcgXml);
		pw.close();
	}

}
