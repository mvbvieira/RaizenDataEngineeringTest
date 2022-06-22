from airflow.operators.bash_operator import BashOperator


class DownloadFileOperator(BashOperator):
    template_fields = ('bash_command', 'source_file', 'target_dir', 'target_file')

    def __init__(self, source_file, target_dir, target_file, *args, **kwargs):

        super(DownloadFileOperator, self).__init__(bash_command="curl -L -o" + target_dir + target_file + " " + source_file,  *args, **kwargs)
        self.source_file = source_file
        self.target_dir = target_dir
        self.target_file = target_file