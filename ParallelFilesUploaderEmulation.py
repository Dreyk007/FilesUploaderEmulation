import queue
from multiprocessing import Pool, Manager, Value
from random import randint
from time import sleep


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


class Uploader:
    """ Класс для имитации загрузки файлов на сервер """

    def __init__(self, files_to_upload, threads_count, reports_q):
        """ Инициализация переменных для дальнейшего использования:
         принимает список файлов для загрузки, количество потоков, очередь для отчётов """
        self.reports_q = reports_q  # Очередь для отчётов
        self.threads_count = threads_count  # Число параллельных потоков
        self.files_to_upload = files_to_upload  # Список файлов для загрузки

        self.total_count = len(files_to_upload)  # Количество файлов для загрузки
        self.processed_count = 0  # Количество обработанных файлов
        self.uploaded_count = 0  # Количество загруженных файлов
        self.errors_count = 0  # Количество произошедших ошибок загрузки
        self.aborted_count = 0  # Количество отменённых загрузок

        self.uploaded_files = []  # Загруженные файлы
        self.error_files = []  # Файлы, которые не удалось загрузить из-за ошибок
        self.aborted_files = []  # Файлы, которые не удалось загрузить из-за принудительной остановки

        self.worker_time = 0.1  # Время имитации нагрузки (загрузки файла)
        self.error_emulation = False  # Включает имитацию случайных ошибок во время загрузки
        self.terminated = False  # Флаг определяющий была ли загрузка принудительно прервана методом '.stop()'
        self.busy = False  # Флаг определяющий продолжается ли загрузка файлов
        self._pool = None  # Переменная для хранения пула

        self._result = []  # Сюда попадают отчёты о работе

    def start(self):
        """ Активация начала загрузки файлов """
        self.busy = True

        # Определение переменных для расшаривания между процессами
        processed_count = Value('i', 0)  # Счётчик количества обработанных файлов
        errors_count = Value('i', 0)  # Счётчик ошибок
        start_trigger = Value('i', False)  # Триггер для определения начала работы пула (workaround)

        # Создание пула с функцией инициализации и расшаренными переменными
        pool = Pool(self.threads_count, initializer=self._initializer, initargs=(self.total_count,
                                                                                 processed_count,
                                                                                 errors_count,
                                                                                 start_trigger))
        # Задачи для пула принимают файлы для загрузки
        jobs = []
        for file in self.files_to_upload:
            jobs.append(pool.apply_async(self._upload, (file,), callback=self._done))

        pool.close()

        # Ожидаем начало работы пула (Workaround для того, чтобы можно было сделать переменную с экземпляром пула
        # свойством класса для использования в других методах:
        while not start_trigger.value:
            sleep(0.1)

        self._pool = pool

    def stop(self):
        """ Метод принудительной остановки загрузки """
        self._pool.terminate()  # Останавливаем пул
        self._pool.join()  # Дожидаемся его завершения

        self._generate_aborted_reports()  # Создаём отчёты для отменённых файлов
        self._calc_result()  # Вызываем подсчёт результатов

        # Устанавливаем флаги состояния
        self.busy = False
        self.terminated = True

    def join(self):
        """ Метод позволяет дождаться заверешения работы аплоадера """
        self._pool.join()

    def is_active(self):
        """ Метод возвращает текущее состояние аплоадера """
        return self.busy

    def _calc_result(self):
        """ Метод высчитывает результаты обработанных файлов и сохраняет их в свойства экземпляра """

        for report in self._result:
            if report.status == 'done':
                self.uploaded_files.append(report.filename)
            elif report.status == 'aborted':
                self.aborted_files.append(report.filename)
            else:
                self.error_files.append(report.filename)

    @property
    def result(self):
        """ Метод возвращает итоговый отчёт о проделанной работе """

        # Получаем и обрабатываем отчёты о работе из переменной с результатами
        uploaded = []
        aborted = []
        error = []
        for report in self._result:
            if report.status == 'done':
                uploaded.append(f'Filename: {report.filename}, status: {report.status}')
            elif report.status == 'aborted':
                aborted.append(f'Filename: {report.filename}, status: {report.status}')
            else:
                error.append(f'Filename: {report.filename}, status: {report.status}: {report.error_message}')

        # Просто возвращаем оформленную строку с результатами
        uploaded_joint = '\n'.join(uploaded)
        error_joint = '\n'.join(aborted)
        aborted_joint = '\n'.join(error)

        result = f"\nTotal results:" \
                 f"\n{'-' * 3}" \
                 f"\nUploaded files:\n{uploaded_joint}" \
                 f"\n{'-' * 3}" \
                 f"\nNot uploaded files:\n{error_joint}\n{aborted_joint}" \
                 f"\n{'-' * 3}" \
                 f"\nTotal files: {self.total_count}" \
                 f"\nDone: {self.uploaded_count}" \
                 f"\nErrors: {self.errors_count}" \
                 f"\nAborted: {self.aborted_count}\n"

        if self.terminated:
            result = '\nWARNING: ABORTED!\n' + result
        elif self.errors_count:
            result = '\nWARNING: ERRORS!\n' + result
        else:
            result = '\nSuccessfully Completed\n' + result

        return result

    def _upload(self, file):
        """ Метод для непосредственной загрузки файлов, который передаётся воркерам:
         возвращает результат в callback-функцию и в очередь """
        # Получаем расшаренные между процессами глобальные переменные
        global START_TRIGGER
        global PROCESSED_COUNT
        global ERRORS_COUNT

        # Сообщаем о начале работы воркеров
        with START_TRIGGER.get_lock():
            if not START_TRIGGER.value:
                START_TRIGGER.value = True

        # Начинаем формирование отчёта
        report = Report()
        report.filename = file
        report.total_count = TOTAL_COUNT
        report.aborted_count = 0
        report.status = 'uploading'

        try:
            # Начало имитации загрузки

            sleep(self.worker_time)

            # Имитация ошибки во время загрузки
            if self.error_emulation:
                if randint(0, 2) == 1:
                    raise ValueError('Emulated error')

            # "Загрузка" окончена
            report.status = 'done'

            # Используя блокировки изменяем значения счётчиков
            with ERRORS_COUNT.get_lock():
                report.errors_count = ERRORS_COUNT.value

            with PROCESSED_COUNT.get_lock():
                PROCESSED_COUNT.value += 1
                report.processed_count = PROCESSED_COUNT.value

                self.reports_q.put(report)  # Добавляем отчёт в очередь

        except Exception as error:
            # Сохраняем информацию о возможной ошибке
            report.status = 'error'
            report.error_message = error

            # Используя блокировки изменяем значения счётчиков
            with PROCESSED_COUNT.get_lock():
                PROCESSED_COUNT.value += 1
                report.processed_count = PROCESSED_COUNT.value

            with ERRORS_COUNT.get_lock():
                ERRORS_COUNT.value += 1
                report.errors_count = ERRORS_COUNT.value

                self.reports_q.put(report)  # Добавляем отчёт в очередь

        finally:
            return report  # Возвращаем результат для callback-функции

    def _done(self, report):
        """ Callback-функция для получения результатов """
        self._result.append(report)  # Сохраняем результат в общий список

        # Обновляем счётчики
        self.processed_count += 1
        if report.status == 'done':
            self.uploaded_count += 1
        else:
            self.errors_count += 1

        if len(self._result) == self.total_count:  # Проверяем: не закончилась ли работа пула
            self._calc_result()  # Вызываем подсчёт результатов
            self.busy = False

    def _generate_aborted_reports(self):
        """ Метод генерирует репорты для отменённых файлов """

        # Вычисляем файлы, которые не были загружены из-за принудительной остановки и создаём отчёты для них
        processed_files = [report.filename for report in self._result]
        aborted_files = list(set(self.files_to_upload) - set(processed_files))
        self.aborted_count = len(aborted_files)

        for file in aborted_files:
            report = Report()
            report.filename = file
            report.status = 'aborted'
            report.total_count = self.total_count
            report.processed_count = self.processed_count
            report.errors_count = self.errors_count
            report.aborted_count = self.aborted_count

            self.reports_q.put(report)
            self._result.append(report)

    @staticmethod
    def _initializer(total_count, processed_count, errors_count, start_trigger):
        """ Функция-инициализатор для пула: расшаривает общие переменные между процессами """
        global TOTAL_COUNT
        global PROCESSED_COUNT
        global ERRORS_COUNT
        global START_TRIGGER

        TOTAL_COUNT = total_count
        PROCESSED_COUNT = processed_count
        ERRORS_COUNT = errors_count
        START_TRIGGER = start_trigger


if __name__ == '__main__':
    # Пример использования:

    files_list = ['file' + str(c) for c in range(20)]  # Генерируем список "файлов" для загрузки

    # Создаём очередь для результатов
    manager = Manager()
    q = manager.Queue()

    uploader = Uploader(files_list, 4, q)  # Создаём экземпляр аплоадера
    uploader.error_emulation = True  # Включаем имитацию случайных ошибок
    uploader.worker_time = 0.5  # Устанавливаем "время загрузки" файлов на сервер
    uploader.start()  # Активируем загрузку
    # uploader.join()  # Дожидаемся остановки без отображения прогресса в режиме реального времени

    # Проверяем результаты в режиме реального времени
    while uploader.is_active():
        try:
            progress = q.get(timeout=1)
            # Случайным образом имитируем принудительную остановку загрузки
            if randint(0, 20) == 1:
                uploader.stop()
        except queue.Empty:
            continue

        # Выводим текущие результаты на экран
        print(f'{progress.filename}: {progress.status}. {progress.progress}')

    # Выводим оставшиеся отчёты
    while not q.empty():
        progress = q.get()
        print(f'{progress.filename}: {progress.status}. {progress.progress}')

    # Формируем и выводим финальный отчёт о результатах работы
    final_result = uploader.result
    print(final_result)
