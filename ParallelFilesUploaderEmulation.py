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
        self.error_message = None

    @property
    def progress(self):
        """ Метод показывает текущий прогресс """
        return f'Processed {self.processed_count} of {self.total_count} files with {self.errors_count} errors.'


class Uploader:
    """ Класс для имитации загрузки файлов на сервер """

    def __init__(self, files_to_upload, threads_count, reports_q):
        """ Инициализация переменных для дальнейшего использования:
         принимает список файлов для загрузки, количество потоков, очередь для отчётов """
        self.reports_q = reports_q  # Очередь для отчётов
        self.threads_count = threads_count  # Число параллельных потоков
        self.files_to_upload = files_to_upload  # Список файлов для загрузки
        self.total_count = len(files_to_upload)  # Количество файлов для загрузки

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
        pool = Pool(self.threads_count, initializer=init, initargs=(self.total_count,
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

        # Вычисляем файлы, который не были загружены из-за принудительной остановки и создаём отчёты для них
        processed_files = [report.filename for report in self._result]
        aborted_files = list(set(self.files_to_upload) - set(processed_files))
        for file in aborted_files:
            report = Report()
            report.filename = file
            report.status = 'aborted'
            self._result.append(report)

        # Устанавливаем флаги состояния
        self.busy = False
        self.terminated = True

    def join(self):
        """ Метод позволяет дождаться заверешения работы аплоадера """
        self._pool.join()

    def is_active(self):
        """ Метод возвращает текущее состояние аплоадера """
        return self.busy

    @property
    def result(self):
        """ Метод возвращает итоговый отчёт о проделанной работе """

        uploaded = []  # Загруженные файлы
        error = []  # Файлы, которые не удалось загрузить из-за ошибок
        aborted = []  # Файлы, которые не удалось загрузить из-за принудительной остановки
        # Получаем и обрабатываем отчёты о работе из переменной с результатами
        for report in self._result:
            if report.status == 'done':
                uploaded.append(f'Filename: {report.filename}, status: {report.status}')
            elif report.status == 'aborted':
                aborted.append(f'Filename: {report.filename}, status: {report.status}')
            else:
                error.append(f'Filename: {report.filename}, status: {report.status}: {report.error_message}')

        # Просто возвращаем оформленную строку с результатами
        uploaded_joint = '\n'.join(uploaded)
        error_joint = '\n'.join(error)
        aborted_joint = '\n'.join(aborted)

        result = f"\nTotal results:" \
                 f"\n{'-' * 3}" \
                 f"\nUploaded files:\n{uploaded_joint}" \
                 f"\n{'-' * 3}" \
                 f"\nNot uploaded files:\n{error_joint}\n{aborted_joint}" \
                 f"\n{'-' * 3}" \
                 f"\nTotal files: {self.total_count}" \
                 f"\nDone: {len(uploaded)}" \
                 f"\nErrors: {len(error)}" \
                 f"\nAborted: {len(aborted)}\n"

        if self.terminated:
            result = '\nWARNING: ABORTED!\n' + result
        elif len(error):
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
        if len(self._result) == self.total_count:  # Проверяем: не закончилась ли работа пула
            self.busy = False


def init(total_count, processed_count, errors_count, start_trigger):
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
    # uploader.join()

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

    # Формируем и выводим финальный отчёт о результатах работы
    final_result = uploader.result
    print(final_result)
