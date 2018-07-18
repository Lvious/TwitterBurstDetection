import stream
import preprocessor
import detection
import tweet_stream
import pdb

tw_stream = tweet_stream.tweetStreamFromCSV('../iecas/event/ts_en_filter.csv')
# tw_stream = tweet_stream.tweetStreamFromCSV('test.csv')

_processor = preprocessor.Preprocessor(tw_stream)

_detection = detection.DetectionComponent(_processor)
count = 0

# pdb.set_trace()
print('hahha')
while True:
    try:
        result = next(_detection)
        count+=1
    except Exception as e:
        raise e
    else:
        print(count)
        if result is stream.End_Of_Stream:
            print("End Stream")
            break
        ptweet,sig = result
        if sig>0:
            print('==============================')
            print(ptweet.__dict__)
            print('==============================')