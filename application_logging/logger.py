from datetime import datetime

class App_Logger:
    def __init__(self):
        pass

    # used to display log msg for easy debugging
    def log(self, file_obj, log_msg):
        self.now = datetime.now()
        self.date = self.now.date()
        self.current_time = self.now.strftime('%H:%M:%S')
        file_obj.write(str(self.date)+"/"+str(self.current_time)+'\t\t'+log_msg+"\n")
