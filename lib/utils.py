import os


class Logs:
    """
    Class for writing logs to a file
    """

    def __init__(self, cwd):
        """
        :param cwd: current work directory (current script folder) - path to the log file
        """
        self.log_file_path = f'{cwd}/output.log'

    # Define a file to record logs of script execution
    def set_config(self):
        """
        Method that sets the configuration of the log file
        The log file is located in the current working directory of the script
        :rtype: Object
        """
        import logging

        return logging.basicConfig(
            filename=self.log_file_path,
            format='%[%(asctime)s] (%(levelname)s): %(message)s',
            level=logging.INFO
        )

    def add_separator(self, cnt=150, sep='-'):
        """
        Method that adds a delimiter to the log file
        :param cnt: number of separators. The default value is 150;
        :param sep: separator type. The default is "-".
        """
        with open(self.log_file_path, "a") as file:
            file.write(sep * cnt + '\n' * 2)

    def name(self, name):
        """
        Method that adds the name of the running script to the log file
        :rtype: object
        :param name: script name
        """
        with open(self.log_file_path, "a") as file:
            file.write(name + '\n' * 2)

    def show_action(self, phrase):
        """
        Method that outputs the current time and the given phrase
        :rtype: object
        :param phrase: console output phrase
        """
        from datetime import datetime

        with open(self.log_file_path, "a") as file:
            file.write(f'[{datetime.now().strftime("%H:%M:%S")}] {phrase}')
