package test.sqoop;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.sqoop.client.SubmissionCallback;
import org.apache.sqoop.model.MSubmission;

public class SqoopJobCallback implements SubmissionCallback {

	private long till = 0L;
	private Logger log = Logger.getLogger(getClass());
	
	@Override
	public void finished(MSubmission msubmission) {
		log.info("this job with id :" + msubmission.getExternalJobId() + " has been finished!");
		log.info("cost time: " + (System.currentTimeMillis()-till)/1000 + "s");
		log.info(msubmission.getStatus().name());
	}

	@Override
	public void submitted(MSubmission msubmission) {
		// TODO Auto-generated method stub
		log.info("job提交状态为：" + msubmission.getStatus());
		till = System.currentTimeMillis();
	}

	@Override
	public void updated(MSubmission msubmission) {
		log.info("当前job的运行状态为：" + msubmission.getStatus().name());
		log.info("进度： " + String.format("%.2f %%", msubmission.getProgress() * 100));
		if(!StringUtils.isEmpty(msubmission.getError().getErrorDetails())){
			log.info(msubmission.getError().getErrorDetails());
		}
	}

}
