import threading
import time
from queue import Queue


class Server:
    def __init__(self, id_):
        self.id = id_
        self.lock = threading.Lock()
        self.task_queue = Queue()
        self.time_left = 0  # Время до освобождения сервера

    def assign_task(self, task_duration):
        '''Назначает задачу серверу, добавляя её в очередь.'''
        with self.lock:
            self.task_queue.put(task_duration)
            self.time_left += task_duration  # Увеличиваем общее время до освобождения
        print(f'Задание с {task_duration} секундами выполнения '
              f'направлено на Сервер {self.id}.')

    def process(self):
        '''Обрабатывает текущую задачу, уменьшая время.'''
        with self.lock:
            if self.time_left > 0:
                self.time_left -= 1
                if not self.task_queue.empty():
                    current_task = self.task_queue.queue[0]  # Берём первое задание в очереди
                    if current_task == 1:
                        print(f'\nСервер {self.id} завершает выполнение задания.')  # Если время выполнения этой задачи истекло
                        self.task_queue.get()  # Удаляем задание из очереди
                        if not self.task_queue.empty():
                            print(f'\nСервер {self.id} приступает к выполнению '
                                  f'задания c {self.task_queue.queue[0]} '
                                  f'секундами выполнения.')
                        else:
                            print(f'\nСервер {self.id} свободен.')
                    else:
                        self.task_queue.queue[0] -= 1  # Уменьшаем оставшееся время для текущей задачи

    def get_status(self):
        '''Возвращает статус сервера.'''
        with self.lock:
            if self.task_queue.empty():
                return f'пусто'
            return (f'выполняет задание '
                    f'(осталось {self.task_queue.queue[0]} сек.)'
                    f'\nочередь Сервера {self.id}: '
                    f'{list(self.task_queue.queue)}')



class DistributedSystem:
    def __init__(self, num_servers):
        self.servers = [Server(i + 1) for i in range(num_servers)]
        self.lock = threading.Lock()
        self.stop_flag = False

    def _get_quickest_server(self):
        '''Находит сервер, который освободится быстрее всех.'''
        return min(self.servers, key=lambda server: server.time_left)

    def add_task(self, task_duration):
        '''Добавляет задачу на сервер с минимальным временем ожидания.'''
        with self.lock:
            quickest_server = self._get_quickest_server()
            quickest_server.assign_task(task_duration)

    def process(self):
        '''Запускает обработку заданий всеми серверами.'''
        while not self.stop_flag:
            for server in self.servers:
                server.process()
            time.sleep(1)

    def get_status(self):
        '''Выводит текущее состояние системы.'''
        print('\nСостояние серверов:')
        for server in self.servers:
            print(f'Сервер {server.id}: {server.get_status()}')
        print()


# Пример использования
if __name__ == '__main__':
    print('Добро пожаловать в симулятор распределенной системы.')
    num_servers = int(input('Введите количество серверов: '))
    if num_servers <=0:
        print('Количество серверов должно быть больше 0.')
        num_servers = int(input('Введите количество серверов: '))
    system = DistributedSystem(num_servers)

    # Запуск системы в отдельном потоке
    system_thread = threading.Thread(target=system.process)
    system_thread.daemon = True
    system_thread.start()

    try:
        while True:
            command = input('Введите команду (добавить X/состояние/выход): ').strip().lower()
            if command.startswith('добавить'):
                try:
                    com, duration = command.split()
                    system.add_task(int(duration))
                except ValueError:
                    print('Введите команду в виде: добавить <количество секунд>')
            elif command == 'состояние':
                system.get_status()
            elif command == "выход":
                print('Вы вышли из симулятора распределенной системы.')
                system.stop_flag = True
                system_thread.join()
                break
            else:
                print('Неизвестная команда. Попробуйте ещё раз.')
    except KeyboardInterrupt:
        print('\nВы вышли из симулятора.')
        system.stop_flag = True
        system_thread.join()
