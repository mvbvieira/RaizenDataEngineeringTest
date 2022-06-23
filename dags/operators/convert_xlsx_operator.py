from airflow.operators.bash_operator import BashOperator


class ConvertXlsxOperator(BashOperator):
    template_fields = ('bash_command', 'source_file', 'target_dir')

    def __init__(self, source_file, target_dir, *args, **kwargs):

        super(ConvertXlsxOperator, self).__init__(bash_command="soffice --convert-to xlsx " + source_file + " --outdir " + target_dir,  *args, **kwargs)
        self.source_file = source_file
        self.target_dir = target_dir