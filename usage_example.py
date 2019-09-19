import queue
from multiprocessing import Manager
from random import randint

from ParallelFilesUploaderEmulation import Uploader

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
