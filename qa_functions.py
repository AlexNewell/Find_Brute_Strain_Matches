import time

def qa_codetimer_start(run_qa_timer: bool):
	if run_qa_timer:
		qa_codetimer_start.starttime = time.perf_counter()


def qa_codetimer_end(run_qa_timer: bool
					 , timertext = ''):
	if run_qa_timer:
		qa_codetimer_end.Time_Elapsed = time.perf_counter() - qa_codetimer_start.starttime
		print('Time Elapsed:'
			  , "{:.2f}".format(qa_codetimer_end.Time_Elapsed)
			  , timertext)
