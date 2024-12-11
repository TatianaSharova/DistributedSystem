import threading
import time
from itertools import count
from queue import PriorityQueue

from exceptions import PriorityValueError, TaskDurationValueError


class Server:
    def __init__(self, id: int):
        self.id = id
        self.lock = threading.Lock()
        # current_task - текущая задача:
        # (priority, task_duration, task_id)
        self.current_task = None
        self.task_id_counter = count(start=1)  # Дает id задачам
        self.task_queue = PriorityQueue()  # Очередь задач с учетом приоритета
        self.time_left = 0  # Время до освобождения сервера

    def assign_task(self, task_duration: int, priority: int):
        '''
        Добавляет задачу в очередь сервера и
        увеличивает время до освобождения сервера.
        '''
        with self.lock:
            task_id = next(self.task_id_counter)
            # добавляем задачу в очередь в виде кортежа
            self.task_queue.put((priority, task_duration, task_id))
            self.time_left += task_duration
        print(f'\nЗадание с {task_duration} секундами выполнения '
              f'направлено на Сервер {self.id}.')

    def _start_next_task(self):
        '''
        Берёт следующую задачу из очереди и начинает её выполнение.
        '''
        if not self.task_queue.empty():
            priority, duration, task_id = self.task_queue.get()
            self.current_task = (priority, duration, task_id)
            print(f'\nСервер {self.id} начал выполнение задачи '
                  f'с длительностью {duration} сек. и приоритетом {priority}.')
        else:
            self.current_task = None
            print(f'\nСервер {self.id} свободен.')

    def process(self):
        '''
        Обрабатывает текущую задачу: уменьшает время выполнения.
        Если задача завершается, то переходит к следующей задаче
        при её наличии.
        '''
        with self.lock:
            if self.current_task:
                # уменьшаем время выполнения
                priority, duration, task_id = self.current_task
                self.current_task = (priority, duration - 1, task_id)
                self.time_left -= 1
                if self.current_task[1] == 0:
                    print(f'Сервер {self.id} завершил задачу.')
                    self.current_task = None
                    self._start_next_task()
            if not self.current_task and not self.task_queue.empty():
                self._start_next_task()

    def get_status(self) -> str:
        '''Возвращает статус сервера.'''
        with self.lock:
            # Берем данные из потока
            current_task = self.current_task
            task_queue = list(self.task_queue.queue)
            time_left = self.time_left

        if not task_queue and not current_task:
            return 'пусто'
        if current_task and task_queue:
            return (f'выполняет задание (осталось {current_task[1]} сек.)\n'
                    f'очередь заданий для Сервера {self.id}: '
                    f'{[task[1] for task in task_queue]}\n'
                    f'Общее время ожидания: {time_left} сек.'
                    f'\n-----------------------------')
        if current_task and not task_queue:
            return (f'выполняет задание (осталось {current_task[1]} сек.)\n'
                    f'очередь заданий для Сервера {self.id}: '
                    f'нет\n'
                    f'Общее время ожидания: {time_left} сек.'
                    f'\n-----------------------------')
        return None


class DistributedSystem:
    def __init__(self, num_servers: int):
        # Создаем список серверов в системе
        self.servers = [Server(i + 1) for i in range(num_servers)]
        self.lock = threading.Lock()
        self.stop_flag = False  # При True останавливаем процесс работы

    def _get_quickest_server(self):
        '''Находит сервер, который освободится быстрее всех.'''
        return min(self.servers, key=lambda server: server.time_left)

    def add_task(self, task_duration: int, priority: int):
        '''Добавляет задачу на сервер с минимальным временем ожидания.'''
        if priority not in range(1, 4):
            raise PriorityValueError(
                'Приоритет должен быть 1 (высокий), '
                '2 (средний) или 3 (низкий).'
            )
        if task_duration < 1:
            raise TaskDurationValueError(
                'Длительность должна быть больше 0!'
            )
        with self.lock:
            quickest_server = self._get_quickest_server()
            quickest_server.assign_task(task_duration, priority)

    def process(self):
        '''Запускает обработку заданий всеми серверами.'''
        while not self.stop_flag:
            for server in self.servers:
                server.process()
            time.sleep(1)  # Симуляция времени

    def get_status(self):
        '''Выводит текущее состояние системы.'''
        print('\nСостояние серверов:')
        for server in self.servers:
            print(f'Сервер {server.id}: {server.get_status()}')
        print()


if __name__ == '__main__':
    print('Добро пожаловать в симулятор распределенной системы.')
    num_servers = int(input('Введите количество серверов: '))
    if num_servers <= 0:
        print('Количество серверов должно быть больше 0.')
        num_servers = int(input('Введите количество серверов: '))
    system = DistributedSystem(num_servers)
    system.get_status()

    # Запуск системы в отдельном потоке управления
    system_thread = threading.Thread(target=system.process, daemon=True)
    system_thread.start()

    try:
        while True:
            command = input('Введите команду '
                            '(добавить X/состояние/выход): ').strip().lower()
            if command.startswith('добавить'):
                try:
                    com, duration = command.split()
                    priority = input(
                        'Введите приоритет задачи '
                        '(1 - высокий, 2 - средний, 3 - низкий): '
                    ).strip()
                    system.add_task(int(duration), int(priority))
                except ValueError:
                    print('Введите команду в виде: добавить <кол-во секунд>')
                except (PriorityValueError, TaskDurationValueError) as error:
                    print(error)
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
