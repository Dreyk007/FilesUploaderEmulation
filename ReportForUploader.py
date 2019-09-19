class Report:
    """ Класс для создания отчётов """

    def __init__(self):
        self.total_count = None
        self.filename = None
        self.status = None
        self.processed_count = None
        self.errors_count = None
        self.aborted_count = None
        self.error_message = None

    @property
    def progress(self):
        """ Метод показывает текущий прогресс """
        return f'Processed {self.processed_count} of {self.total_count} files with {self.errors_count} errors ' \
               f'and {self.aborted_count} aborted.'
