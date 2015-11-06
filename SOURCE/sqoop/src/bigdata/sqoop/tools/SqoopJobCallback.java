package bigdata.sqoop.tools;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.sqoop.client.SubmissionCallback;
import org.apache.sqoop.model.MSubmission;

public class SqoopJobCallback implements SubmissionCallback {

	private static Logger log = Logger.getLogger(SqoopJobCallback.class);
	
	private long till = 0L;
	
	@Override
	
	
	public void finished(MSubmission msubmission) {
		
		if(msubmission.getStatus().isFailure()){
			log.info(String.format("this job id :%s ; error detail:%s",msubmission.getExternalJobId(),
					msubmission.getError().getErrorDetails()));
		}else{
			log.info("this job with id :" + msubmission.getExternalJobId() + " status " + msubmission.getStatus().name());
			log.info("cost time: " + (System.currentTimeMillis()-till)/1000 + "s");
		}
		
	}

	@Override
	public void submitted(MSubmission msubmission) {
		// TODO Auto-generated method stub
		log.info("job "+msubmission.getExternalJobId()+" 提交状态为：" + msubmission.getStatus());
		till = System.currentTimeMillis();
	}

	@Override
	public void updated(MSubmission msubmission) {
		log.info("当前job "+msubmission.getExternalJobId()+" 的运行状态为：" + msubmission.getStatus().name());
		
		log.info("进度： " + String.format("%.2f %%", msubmission.getProgress() * 100));
		if(!StringUtils.isEmpty(msubmission.getError().getErrorDetails())){
			log.info(String.format("this job id :%s ; error detail:%s",msubmission.getExternalJobId(),
					msubmission.getError().getErrorDetails()));
		}
	}

}
