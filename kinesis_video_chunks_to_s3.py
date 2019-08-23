import cv2
import boto3
import time
from datetime import datetime
from datetime import timedelta


# enter the required details below
AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
REGION_NAME = ''
STREAM_NAME = ''

def save_chunks_to_s3(init_time, time_range):

	timestamp = datetime.strptime(init_time, "%Y%m%d_%H%M%S")
	print(timestamp)

	while True:
		
		playback_url = kvam.get_hls_streaming_session_url(StreamName=STREAM_NAME, PlaybackMode='ON_DEMAND', 
			HLSFragmentSelector={
            'FragmentSelectorType': 'SERVER_TIMESTAMP',
            'TimestampRange': {
                'StartTimestamp': timestamp,
                'EndTimestamp': timestamp + timedelta(seconds=time_range)
            	}
        	}
    	)['HLSStreamingSessionURL']

		vcap = cv2.VideoCapture(playback_url)

		fwidth = int(vcap.get(cv2.CAP_PROP_FRAME_WIDTH)) 
		fheight = int(vcap.get(cv2.CAP_PROP_FRAME_HEIGHT))

		length = int(vcap.get(cv2.CAP_PROP_FRAME_COUNT))

		current_time = timestamp.strftime("%Y%m%d_%H%M%S")
		filename = 'output_'+current_time+'.avi'

		print("Saving to file "+filename)

		fourcc = cv2.VideoWriter_fourcc(*'MJPG')
		out = cv2.VideoWriter(filename,fourcc, 20.0, (fwidth,fheight))

		(grabbed, frame) = vcap.read()

		if grabbed:
			while(length>0):
				if frame is not None:
					out.write(frame)
				length -= 1
				print("Frames pending: "+str(length))
				(grabbed, frame) = vcap.read()

			out.release()
			vcap.release()

			with open(filename, 'rb') as data:
				s3.upload_fileobj(data, 'android-kinesis-video-chunks', 'android_stream_'+filename)


			print("Saved to file "+filename)

			timestamp = timestamp + timedelta(seconds=time_range)

		else:
			out.release()
			vcap.release()
			break



s3 = boto3.client("s3", 
                       region_name=REGION_NAME, 
                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

kvs = boto3.client("kinesisvideo", 
       region_name=REGION_NAME, 
       aws_access_key_id=AWS_ACCESS_KEY_ID,
       aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

response = kvs.get_data_endpoint(APIName="GET_HLS_STREAMING_SESSION_URL", StreamName=STREAM_NAME)

# Grab the endpoint from GetDataEndpoint
endpoint = response['DataEndpoint']

# Grab the HLS Stream URL from the endpoint
kvam = boto3.client("kinesis-video-archived-media", 
        endpoint_url=endpoint, 
        region_name=REGION_NAME, 
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

while True:
	try:
		live_url = kvam.get_hls_streaming_session_url(StreamName=STREAM_NAME, PlaybackMode="LIVE")['HLSStreamingSessionURL']
		init_utc_time = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

		start_time = time.time()

		while(time.time()-start_time<90):
			print(time.time()-start_time)

		save_chunks_to_s3(init_utc_time, 60)

	
	except:
		print("Waiting for live stream......")
