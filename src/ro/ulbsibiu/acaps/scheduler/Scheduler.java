package ro.ulbsibiu.acaps.scheduler;

import ro.ulbsibiu.acaps.ctg.xml.apcg.ApcgType;

/**
 * <p>
 * A scheduler has the responsibility to assign tasks to cores and to specify
 * how and when a core executes its assigned tasks.
 * </p>
 * 
 * <p>
 * Such a scheduler is used for the scheduling problem from the field of
 * Network-on-Chip (NoCs).
 * </p>
 * 
 * @author cipi
 * 
 */
public interface Scheduler {

	/**
	 * Schedules the CTG tasks to the available cores.
	 * 
	 * @see ApcgType
	 * 
	 * @return a String containing the APCG XML
	 */
	public abstract String schedule();

}