package ro.ulbsibiu.acaps.scheduler.direct;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.log4j.Logger;

import ro.ulbsibiu.acaps.ctg.xml.apcg.ApcgType;
import ro.ulbsibiu.acaps.ctg.xml.apcg.ObjectFactory;
import ro.ulbsibiu.acaps.ctg.xml.core.CoreType;
import ro.ulbsibiu.acaps.ctg.xml.task.TaskType;
import ro.ulbsibiu.acaps.scheduler.Scheduler;

/**
 * This @link{Scheduler} directly assigns tasks to cores: task 0 is assigned to
 * core 0, task 1 is assigned to core 1 etc.
 * 
 * @author cipi
 * 
 */
public class DirectScheduler implements Scheduler {

	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger
			.getLogger(DirectScheduler.class);
	
	private static final String SCHEDULER_ID = "1";

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
	public DirectScheduler(String ctgId, String tasksFilePath,
			String coresFilePath) {
		logger.assertLog(ctgId != null && ctgId.length() > 0,
				"A CTG must be specified");
		logger.assertLog(tasksFilePath != null && tasksFilePath.length() > 0,
				"A tasks file path must be specified");
		logger.assertLog(coresFilePath != null && coresFilePath.length() > 0,
				"A tasks file path must be specified");

		this.apcgId = ctgId + "_" + getSchedulerId();
		logger.assertLog(apcgId != null && apcgId.length() > 0,
				"An APCG must be specified");
		
		this.ctgId = ctgId;

		File tasksFile = new File(tasksFilePath);
		logger.assertLog(tasksFile.isDirectory(),
				"The tasks file path doesn't point a directory");

		File coresFile = new File(coresFilePath);
		logger.assertLog(coresFile.isDirectory(),
				"The cores file path doesn't point a directory");

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

	@Override
	public String getSchedulerId() {
		return SCHEDULER_ID;
	}

	private int findCoreIndex(String coreId) throws JAXBException {
		int coreIndex = -1;
		logger.debug("Searching for a core with ID " + coreId);
		for (int i = 0; i < coreXmls.length; i++) {
			CoreType core = getCore(coreXmls[i]);
			if (core.getID().equals(coreId)) {
				coreIndex = i;
				logger.debug("Found core " + coreXmls[i] + " for task "
						+ coreId + " at index " + i);
				break;
			}
		}
		return coreIndex;
	}

	/**
	 * Schedules the CTG tasks to the available cores in a direct fashion: task
	 * 0 is assigned to core 0, task 1 is assigned to core 1 etc.
	 * 
	 * @see ApcgType
	 * 
	 * @return a String containing the APCG XML
	 */
	@Override
	public String schedule() {
		if (logger.isDebugEnabled()) {
			logger.debug("Direct scheduling started");
		}

		tasksToCores = new HashMap<File, File>(taskXmls.length);
		for (int i = 0; i < taskXmls.length; i++) {
			int coreIndex = -1;
			String taskId = null;
			try {
				taskId = getTask(taskXmls[i]).getID();
				coreIndex = findCoreIndex(taskId);
			} catch (JAXBException e) {
				logger.error("JAXB encountered an error", e);
			}
			if (coreIndex == -1) {
				logger.assertLog(
						coreIndex == -1,
						"The direct scheduler requires a core to be available for each task. However, task "
								+ taskId
								+ " does not have a corresponding core "
								+ taskId + "!");
			} else {
				if (logger.isInfoEnabled()) {
					logger.info("Task " + i + " is scheduled to core " + i);
				}
				logger.debug("Assigning task " + taskXmls[i] + " to core "
						+ coreXmls[coreIndex]);
				tasksToCores.put(taskXmls[i], coreXmls[coreIndex]);
			}
		}
		String apcgXml = null;
		try {
			apcgXml = generateApcg();
		} catch (JAXBException e) {
			logger.error("JAXB encountered an error", e);
		}

		if (logger.isDebugEnabled()) {
			logger.debug("Direct scheduling finished");
		}

		return apcgXml;
	}

	private CoreType getCore(File file) throws JAXBException {
		JAXBContext jaxbContext = JAXBContext
				.newInstance("ro.ulbsibiu.acaps.ctg.xml.core");
		Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
		@SuppressWarnings("unchecked")
		JAXBElement<CoreType> coreXml = (JAXBElement<CoreType>) unmarshaller
				.unmarshal(file);
		return coreXml.getValue();
	}

	private TaskType getTask(File file) throws JAXBException {
		JAXBContext jaxbContext = JAXBContext
				.newInstance("ro.ulbsibiu.acaps.ctg.xml.task");
		Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
		@SuppressWarnings("unchecked")
		JAXBElement<TaskType> taskXml = (JAXBElement<TaskType>) unmarshaller
				.unmarshal(file);
		return taskXml.getValue();
	}

	private ro.ulbsibiu.acaps.ctg.xml.core.TaskType getCoreTask(
			List<ro.ulbsibiu.acaps.ctg.xml.core.TaskType> tasks, String type) {
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
			// core UID = core ID = task ID (only one task per core) 
			coreType.setUid(coreId);
			coreType.setId(coreId);
			for (File task : set) {
				String taskId = getTask(task).getID();
				ro.ulbsibiu.acaps.ctg.xml.apcg.TaskType taskType = new ro.ulbsibiu.acaps.ctg.xml.apcg.TaskType();
				taskType.setId(taskId);
				taskType.setExecTime(getCoreTask(getCore(core).getTask(),
						getTask(task).getType()).getExecTime());
				taskType.setPower(getCoreTask(getCore(core).getTask(),
						getTask(task).getType()).getPower());
				coreType.getTask().add(taskType);
			}
			apcgType.getCore().add(coreType);
		}

		JAXBContext jaxbContext = JAXBContext.newInstance(ApcgType.class);
		Marshaller marshaller = jaxbContext.createMarshaller();
		marshaller.setProperty("jaxb.formatted.output", Boolean.TRUE);
		StringWriter stringWriter = new StringWriter();
		JAXBElement<ApcgType> apcg = apcgFactory.createApcg(apcgType);
		marshaller.marshal(apcg, stringWriter);

		return stringWriter.toString();
	}

	public static void main(String[] args) throws FileNotFoundException {
		System.err.println("usage:   java DirectScheduler.class [E3S benchmarks]");
		System.err.println("note:	 each CTG is only scheduled individually (e.g.: folders named like ctg-0+1 are ignored)");
		System.err.println("example 1 (specify the tgff file): java DirectScheduler.class ../CTG-XML/xml/e3s/auto-indust-mocsyn.tgff ../CTG-XML/xml/e3s/telecom-mocsyn.tgff");
		System.err.println("example 2 (schedule the entire E3S benchmark suite): java DirectScheduler.class");
		File[] tgffFiles = null;
		if (args == null || args.length == 0) {
			File e3sDir = new File(".." + File.separator + "CTG-XML"
					+ File.separator + "xml" + File.separator + "e3s");
			logger.assertLog(e3sDir.isDirectory(),
					"Could not find the E3S benchmarks directory!");
			tgffFiles = e3sDir.listFiles(new FilenameFilter() {

				@Override
				public boolean accept(File dir, String name) {
					return name.endsWith(".tgff");
				}
			});
		} else {
			tgffFiles = new File[args.length];
			for (int i = 0; i < args.length; i++) {
				tgffFiles[i] = new File(args[i]);
			}
		}
		for (int i = 0; i < tgffFiles.length; i++) {
			String path = tgffFiles[i].getPath() + File.separator;
			File e3sBenchmark = tgffFiles[i];
			String[] ctgs = e3sBenchmark.list(new FilenameFilter() {

				@Override
				public boolean accept(File dir, String name) {
					return dir.isDirectory() && name.startsWith("ctg-");
				}
			});
			for (int j = 0; j < ctgs.length; j++) {
				String ctgId = ctgs[j].substring("ctg-".length());
				if (!ctgId.contains("+")) {
					Scheduler scheduler = new DirectScheduler(ctgId, path + "ctg-"
							+ ctgId + File.separator + "tasks", path + "cores");
					String apcgId = ctgId + "_" + scheduler.getSchedulerId();
					String apcgXml = scheduler.schedule();
					String xmlFileName = path + "ctg-" + ctgId + File.separator
							+ "apcg-" + apcgId + ".xml";
					PrintWriter pw = new PrintWriter(xmlFileName);
					logger.info("Saving the scheduling XML file " + xmlFileName);
					pw.write(apcgXml);
					pw.close();
				}
			}
			logger.info("Finished with e3s" + File.separator
					+ tgffFiles[i].getName());
		}
		logger.info("Done.");
	}

}
