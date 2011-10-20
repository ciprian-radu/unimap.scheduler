package ro.ulbsibiu.acaps.scheduler.fadse;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;

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
 * This @link{Scheduler} starts from already generated APCGs (with another
 * {@link Scheduler}) and changes the core types based on the output given by
 * FADSE (https://code.google.com/p/fadse/). When we search with FADSE for
 * System-on-Chip optimal configurations, we get for each solution a string
 * like:
 * "core-0_0=12|core-0_1=32|core-0_2=33|core-0_3=29|core-1_0=5|core-1_1=26|core-1_2=33|core-1_3=32|core-1_4=8|core-1_5=22|core-2_0=32|core-2_1=15|core-2_2=33|core-2_3=32|core-2_4=27|core-2_5=8|core-3_0=33|core-3_1=32|core-3_2=22|core-4_0=26|core-4_1=32|core-4_2=22|core-5_0=32|core-5_1=32|core-6_0=30|core-6_1=32|core-7_0=13|core-7_1=15|core-8_0=5|core-8_1=32"
 * . For each item "core-x_y=z" x is the CTG ID, y is the core UID from the
 * corresponding APCG and z is the core ID (we work with the E3S core library).
 * For more details, see the FADSE code that integrates UniMap into FADSE.
 * 
 * @author cipi
 * 
 */
public class FadseOutputBasedScheduler implements Scheduler {

	/**
	 * Logger for this class
	 */
	private static final Logger logger = Logger
			.getLogger(FadseOutputBasedScheduler.class);

	private static final String SCHEDULER_ID = "3";

	/** the ID of the Application Characterization Graph */
	private String apcgId;

	/** the ID of the Communication Task Graph */
	private String ctgId;
	
	/** the APCG XMl file that will be used by FADSE to create another APCG, by changing the core type */
	private String templateApcgFilePath;
	
	/** the output from FADSE, which contains the core types */
	private String outputFromFadse;

	/** the XML files containing the tasks */
	private File[] taskXmls;

	/** the XML files containing the cores */
	private File[] coreXmls;

	/**
	 * Constructor
	 * 
	 * @param templateApcgFilePath
	 *            the APCG XMl file that will be used by FADSE to create another
	 *            APCG, by changing the core type
	 * @param outputFromFadse
	 *            the output from FADSE, which contains the core types
	 * @param ctgId
	 *            the ID of the Communication Task Graph (cannot be empty)
	 * @param tasksFilePath
	 *            the XML files containing the tasks (cannot be empty)
	 * @param coresFilePath
	 *            the XML files containing the cores (cannot be empty)
	 */
	public FadseOutputBasedScheduler(String templateApcgFilePath, String outputFromFadse, String ctgId, String tasksFilePath,
			String coresFilePath) {
		logger.assertLog(templateApcgFilePath != null && templateApcgFilePath.length() > 0,
				"A template APCG file path must be specified");
		logger.assertLog(outputFromFadse != null && outputFromFadse.length() > 0,
				"The output from FADSE is required");
		logger.assertLog(ctgId != null && ctgId.length() > 0,
				"A CTG must be specified");
		logger.assertLog(tasksFilePath != null && tasksFilePath.length() > 0,
				"A tasks file path must be specified");
		logger.assertLog(coresFilePath != null && coresFilePath.length() > 0,
				"A tasks file path must be specified");

		this.templateApcgFilePath = templateApcgFilePath;
		this.outputFromFadse = outputFromFadse;
		
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
	}

	@Override
	public String getSchedulerId() {
		return SCHEDULER_ID;
	}

	private String findTaskType(String taskId) throws JAXBException {
		String taskType = null;
		if (logger.isDebugEnabled()) {
			logger.debug("Searching for a task with ID " + taskId);
		}
		for (int i = 0; i < taskXmls.length; i++) {
			TaskType task = getTask(taskXmls[i]);
			if (task.getID().equals(taskId)) {
				taskType = task.getType();
				if (logger.isDebugEnabled()) {
					logger.debug("Found task type " + taskType + " for task "
							+ taskId);
				}
				break;
			}
		}
		return taskType;
	}
	
	private CoreType findCoreType(String coreId) throws JAXBException {
		CoreType core = null;
		if (logger.isDebugEnabled()) {
			logger.debug("Searching for a core with ID " + coreId);
		}
		for (int i = 0; i < coreXmls.length; i++) {
			core = getCore(coreXmls[i]);
			if (core.getID().equals(coreId)) {
				break;
			}
		}
		return core;
	}

	private String findNewCoreId(String coreUid) {
		String pattern = "core-" + ctgId + "_" + coreUid + "=";
		int idx = outputFromFadse.indexOf(pattern);
		logger.assertLog(idx >= 0, "Could not find in FADSE output ("
				+ outputFromFadse + ") core with UID " + coreUid + ", for CTG "
				+ ctgId);
		String coreId = outputFromFadse.substring(idx + pattern.length());
		coreId = coreId.substring(0, coreId.indexOf("|"));
		if (logger.isDebugEnabled()) {
			logger.debug("Found for CTG " + ctgId + " and core UID " + coreUid
					+ " core ID " + coreId + "");
		}
		return coreId;
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
			logger.debug("FADSE output based scheduling started");
			logger.debug("Template APCG XML file is " + templateApcgFilePath);
			logger.debug("Output from FADSE is " + outputFromFadse);
		}

		String apcgXml = null;
		try {
			JAXBContext jaxbContext = JAXBContext.newInstance("ro.ulbsibiu.acaps.ctg.xml.apcg");
			Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
			@SuppressWarnings("unchecked")
			JAXBElement<ApcgType> templateApcgElem = (JAXBElement<ApcgType>) unmarshaller
					.unmarshal(new File(templateApcgFilePath));
			ApcgType templateApcg = templateApcgElem.getValue();
			templateApcg.setId(ctgId + "_" + SCHEDULER_ID);
			List<ro.ulbsibiu.acaps.ctg.xml.apcg.CoreType> coreList = templateApcg.getCore();
			for (ro.ulbsibiu.acaps.ctg.xml.apcg.CoreType coreType : coreList) {
				coreType.setId(findNewCoreId(coreType.getUid()));
				List<ro.ulbsibiu.acaps.ctg.xml.apcg.TaskType> taskList = coreType.getTask();
				for (ro.ulbsibiu.acaps.ctg.xml.apcg.TaskType taskType : taskList) {
					String type = findTaskType(taskType.getId());
					ro.ulbsibiu.acaps.ctg.xml.core.TaskType coreTask = getCoreTask(findCoreType(coreType.getId()).getTask(), type);
					taskType.setExecTime(coreTask.getExecTime());
					taskType.setPower(coreTask.getPower());
				}
			}
			
			Marshaller marshaller = jaxbContext.createMarshaller();
			marshaller.setProperty("jaxb.formatted.output", Boolean.TRUE);
			StringWriter stringWriter = new StringWriter();
			ObjectFactory apcgFactory = new ObjectFactory();
			JAXBElement<ApcgType> apcg = apcgFactory.createApcg(templateApcg);
			marshaller.marshal(apcg, stringWriter);

			apcgXml = stringWriter.toString();
		} catch (JAXBException e) {
			logger.error("JAXB encountered an error", e);
		}

		if (logger.isDebugEnabled()) {
			logger.debug("FADSE output based scheduling finished");
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

	public static void main(String[] args) throws FileNotFoundException {
		System.err
				.println("usage:   java FadseOutputBasedScheduler.class {application file path} {APCG ID} {FADSE output}");
		System.err
				.println("note:	 each CTG is only scheduled individually (e.g.: folders named like ctg-0+1 are ignored)");
		System.err
				.println("example: java FadseOutputBasedScheduler.class /home/cradu/workspace/CTG-XML/xml/e3s/telecom-mocsyn.tgff 2 core-0_0=12|core-0_1=32|core-0_2=33|core-0_3=29|core-1_0=5|core-1_1=26|core-1_2=33|core-1_3=32|core-1_4=8|core-1_5=22|core-2_0=32|core-2_1=15|core-2_2=33|core-2_3=32|core-2_4=27|core-2_5=8|core-3_0=33|core-3_1=32|core-3_2=22|core-4_0=26|core-4_1=32|core-4_2=22|core-5_0=32|core-5_1=32|core-6_0=30|core-6_1=32|core-7_0=13|core-7_1=15|core-8_0=5|core-8_1=32|");
		if (args == null || args.length != 3) {
			logger.error("This scheduler must be invoked with 3 parameters!");
		} else {
			File application = new File(args[0]);
			String path = application.getPath() + File.separator;
			String[] ctgs = application.list(new FilenameFilter() {

				@Override
				public boolean accept(File dir, String name) {
					return dir.isDirectory() && name.startsWith("ctg-");
				}
			});
			for (int j = 0; j < ctgs.length; j++) {
				String ctgId = ctgs[j].substring("ctg-".length());
				if (!ctgId.contains("+")) {
					Scheduler scheduler = new FadseOutputBasedScheduler(new File(path + "ctg-" + ctgId + File.separator
							+ "apcg-" + ctgId + "_" + args[1] + ".xml").getPath(), args[2], ctgId,
							path + "ctg-" + ctgId + File.separator + "tasks",
							path + "cores");
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
			logger.info("Done.");
		}
	}

}
