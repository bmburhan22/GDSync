from gdsync import GDSync, time
sync_time = 1
while True:
    with GDSync() as service:
        service.run()
    time.sleep(sync_time)