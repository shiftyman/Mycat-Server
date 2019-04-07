package org.opencloudb.sqlengine;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.opencloudb.MycatServer;

public class BatchSQLJob {

	private ConcurrentHashMap<Integer, SQLJob> runningJobs = new ConcurrentHashMap<Integer, SQLJob>();
	private ConcurrentLinkedQueue<SQLJob> waitingJobs = new ConcurrentLinkedQueue<SQLJob>();
	private volatile boolean noMoreJobInput = false;

	public void addJob(SQLJob newJob, boolean parallExecute) {

		// 这个有点扯淡，没有独立的线程/线程池去做job处理，而是靠addJob的线程来run。
		// 假如并行，那么当前线程直接run，因为多线程并发到达，所以这里相当于多线程并行地run各自的job
		// 假如不并行，那么这里先放到队列waitingJobs中，如果当前无任务run，则run一个任务，这样会达到单线程顺序run的效果，
		// 但是可能会有一个job一直囤积直到下一个addjob发生
		if (parallExecute) {
			runJob(newJob);
		} else {
			waitingJobs.offer(newJob);
			if (runningJobs.isEmpty()) {
				SQLJob job = waitingJobs.poll();
				if (job != null) {
					runJob(job);
				}
			}
		}
	}

	public void setNoMoreJobInput(boolean noMoreJobInput) {
		this.noMoreJobInput = noMoreJobInput;
	}

	private void runJob(SQLJob newJob) {
		// EngineCtx.LOGGER.info("run job " + newJob);
		runningJobs.put(newJob.getId(), newJob);
		MycatServer.getInstance().getBusinessExecutor().execute(newJob);
	}

	public boolean jobFinished(SQLJob sqlJob) {
		if (EngineCtx.LOGGER.isDebugEnabled()) {
			EngineCtx.LOGGER.info("job finished " + sqlJob);
		}
		runningJobs.remove(sqlJob.getId());
		SQLJob job = waitingJobs.poll();
		if (job != null) {
			runJob(job);
			return false;
		} else {
			if (noMoreJobInput) {
				return runningJobs.isEmpty() && waitingJobs.isEmpty();
			} else {
				return false;
			}
		}

	}
}
