package ro.ulbsibiu.acaps.scheduler.random;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
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
import ro.ulbsibiu.acaps.scheduler.Scheduler;

/**
 * This @link{Scheduler} assigns tasks to available cores in a random fashion.
 * 
 * @author cipi
 * 
 */
public class RandomScheduler implements Scheduler {
	
	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger
			.getLogger(RandomScheduler.class);

	/** the ID of the Application Characterization Graph */
	private String apcgId;
	
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
	public RandomScheduler(String apcgId, String ctgId, String tasksFilePath,
			String coresFilePath) {
		logger.assertLog(apcgId != null && apcgId.length() > 0,
				"An APCG must be specified");
		logger.assertLog(ctgId != null && ctgId.length() > 0,
				"A CTG must be specified");
		logger.assertLog(tasksFilePath != null && tasksFilePath.length() > 0,
				"A tasks file path must be specified");
		logger.assertLog(coresFilePath != null && coresFilePath.length() > 0,
				"A tasks file path must be specified");

		this.apcgId = apcgId;
		this.ctgId = ctgId;

		File tasksFile = new File(tasksFilePath);
		logger.assertLog(tasksFile.isDirectory(),
				"The tasks file path doesn't point a directory");

		File coresFile = new File(coresFilePath);
		logger.assertLog(coresFile.isDirectory(),
				"The tasks file path doesn't point a directory");

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
		if (logger.isDebugEnabled()) {
			logger.debug("Random scheduling started");
		}
		
		tasksToCores = new HashMap<File, File>(taskXmls.length);
		Random random = new Random();
		for (int i = 0; i < taskXmls.length; i++) {
			int t = random.nextInt(coreXmls.length);
			if (logger.isInfoEnabled()) {
				logger.info("Task " + i + " is scheduled to core " + t);
			}
			tasksToCores.put(taskXmls[i], coreXmls[t]);
		}
		String apcgXml = null;
		try {
			apcgXml = generateApcg();
		} catch (JAXBException e) {
			logger.error("JAXB encountered an error", e);
		}
		
		if (logger.isDebugEnabled()) {
			logger.debug("Random scheduling finished");
		}
		
		return apcgXml;
	}

	private CoreType getCore(File file) throws JAXBException {
		JAXBContext jaxbContext = JAXBContext.newInstance("ro.ulbsibiu.acaps.ctg.xml.core");
		Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
		@SuppressWarnings("unchecked")
		JAXBElement<CoreType> coreXml = (JAXBElement<CoreType>) unmarshaller.unmarshal(file);
		return coreXml.getValue();
	}
	
	private TaskType getTask(File file) throws JAXBException {
		JAXBContext jaxbContext = JAXBContext.newInstance("ro.ulbsibiu.acaps.ctg.xml.task");
		Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
		@SuppressWarnings("unchecked")
		JAXBElement<TaskType> taskXml = (JAXBElement<TaskType>) unmarshaller.unmarshal(file);
		return taskXml.getValue();
	}
	
	private ro.ulbsibiu.acaps.ctg.xml.core.TaskType getCoreTask(List<ro.ulbsibiu.acaps.ctg.xml.core.TaskType> tasks, String type) {
		ro.ulbsibiu.acaps.ctg.xml.core.TaskType theTaskType = null;
		for (ro.ulbsibiu.acaps.ctg.xml.core.TaskType taskType : tasks) {
			if (type.equals(taskType.getType())) {
				theTaskType = taskType;
				break;
			}
		}
		return theTaskType;
	}
	
	private String generateApcg() throws JAXBException {
		logger.assertLog(tasksToCores != null, "No task was scheduled!");

		if (logger.isDebugEnabled()) {
			logger.debug("Generating an XML String with the scheduling");
		}

		ObjectFactory apcgFactory = new ObjectFactory();
		ApcgType apcgType = new ApcgType();
		apcgType.setId(apcgId);
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
			String coreId = getCore(core).getID();
			Set<File> set = coreToTasks.get(core);
			ro.ulbsibiu.acaps.ctg.xml.apcg.CoreType coreType = new ro.ulbsibiu.acaps.ctg.xml.apcg.CoreType();
			coreType.setId(coreId);
			for (File task : set) {
				String taskId = getTask(task).getID();
				ro.ulbsibiu.acaps.ctg.xml.apcg.TaskType taskType = new ro.ulbsibiu.acaps.ctg.xml.apcg.TaskType();
				taskType.setId(taskId);
				taskType.setExecTime(getCoreTask(getCore(core).getTask(), getTask(task).getType()).getExecTime());
				taskType.setPower(getCoreTask(getCore(core).getTask(), getTask(task).getType()).getPower());
				coreType.getTask().add(taskType);
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
		String e3sBenchmark = "auto-indust-mocsyn.tgff";
		
		for (int i = 0; i < 4; i++) {
			String ctgId = Integer.toString(i);
			String apcgId = ctgId + "_0";
			String path = "../CTG-XML" + File.separator + "xml"
					+ File.separator + "e3s" + File.separator + e3sBenchmark
					+ File.separator;
			Scheduler scheduler = new RandomScheduler(apcgId, ctgId, path
					+ "ctg-" + ctgId + File.separator + "tasks", path + "cores");
			String apcgXml = scheduler.schedule();
			PrintWriter pw = new PrintWriter(path + "ctg-" + ctgId
					+ File.separator + "apcg-" + apcgId + ".xml");
			logger.info("Saving the scheduling XMl file");
			pw.write(apcgXml);
			pw.close();
		}
	}

}
