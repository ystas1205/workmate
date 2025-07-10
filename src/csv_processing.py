import argparse
from csv import DictReader
from tabulate import tabulate


class FileValuesProcessor:
    def __init__(self):
        self.filtered_data = []
        self.d = None

    @staticmethod
    def parse_condition(data_operations: str, operator: str) -> tuple:
        """ Разбиение строки на две части: колонку и значение"""

        column, value = data_operations.split(operator)
        return column, value

    def read_file_csv(self,
                      file: str,
                      where: str = None,
                      aggregate: str = None,
                      order_by: str = None) -> None:

        """Читает данные из файла и передаёт их в функции для обработки"""
        with open(file, mode="r", encoding="utf-8", newline="") as f:
            reader = DictReader(f)
            list_reader = list(reader)

            if not where and not aggregate and not order_by:
                print(tabulate(list_reader, headers="keys", tablefmt="grid"))

            if where:
                self.filter_data(list_reader=list_reader, where=where)

            if order_by:
                self.order_by_data(list_reader, order_by=order_by)

            if aggregate:
                self.aggregate_data(list_reader, aggregate=aggregate)

    def filter_data(self, list_reader: list[dict],
                    where: str) -> None:

        """ Фильтрация """

        if '=' in where:
            # разбиение входящих данных (условия фильтрации) на две части

            column, value = self.parse_condition(where, '=')

            self.filtered_data = [row for row in list_reader if
                                  (row[column]) == value]


        elif '>' in where:

            # разбиение входящих данных (условия фильтрации) на две части
            column, value = self.parse_condition(where, '>')

            # проверка можно ли преобразовать строку в число
            try:
                value = float(value)

            except (ValueError, TypeError):
                raise ValueError(
                    "Фильтрация по условию больше требует числовых значений.")
            self.filtered_data = [row for row in list_reader if
                                  float(row[column]) > value]

        elif '<' in where:
            # разбиение входящих данных (условия фильтрации) на две части
            column, value = self.parse_condition(where, '<')

            # проверка можно ли преобразовать строку в число
            try:
                value = float(value)

            except (ValueError, TypeError):
                raise ValueError(
                    "Фильтрация по условию меньше требует числовых значений..")

            self.filtered_data = [row for row in list_reader if
                                  float(row[column]) < value]


        else:
            raise ValueError(
                "Недопустимый оператор в фильтрации. Допустимые операторы: '=', '<', '>'.")

        if not self.filtered_data:
            raise ValueError(
                "Не найдено записей, соответствующих условиям фильтрации.")

    def order_by_data(self, list_reader: list[dict],
                      order_by: str) -> None:

        """Сортировка"""

        if "=" in order_by:

            # разбиение входящих данных (условия сортировки) на две части
            column, value = self.parse_condition(order_by, '=')

            # Данные для проверки, является ли значение
            # в условии сортировки числовым значением
            data = list_reader[0]

            # проверка является ли колонка числом
            has_digit = any(char.isdigit() for char in data[column])
            if value == 'desc':

                if has_digit:

                    # при наличии фильтрации берём для обработки отфильтрованные данные
                    if self.filtered_data:

                        # если колонка фильтрации является числом преобразуем его в float
                        # для дальнейшей обработки

                        self.filtered_data = sorted(self.filtered_data,
                                                    key=lambda x: float(
                                                        x[column]),
                                                    reverse=True)

                    # если нет данных берем данные из файла
                    else:
                        self.filtered_data = sorted(list_reader,
                                                    key=lambda x: float(
                                                        x[column]),
                                                    reverse=True)

                else:
                    # если колонка строка, при наличии фильтрации берём для
                    # обработки отфильтрованные данные
                    if self.filtered_data:

                        self.filtered_data = sorted(self.filtered_data,
                                                    key=lambda x:
                                                    x[column],
                                                    reverse=True)

                    # если нет отфильтрованных берем данные из файла
                    else:
                        self.filtered_data = sorted(list_reader,
                                                    key=lambda x:
                                                    x[column],
                                                    reverse=True)

            # применяем ту же логику для порядка сортировки asc
            elif value == "asc":

                if has_digit:

                    if self.filtered_data:

                        self.filtered_data = sorted(self.filtered_data,
                                                    key=lambda x: float(
                                                        x[column]),
                                                    reverse=False)

                    else:
                        self.filtered_data = sorted(list_reader,
                                                    key=lambda x: float(
                                                        x[column]),
                                                    reverse=False)

                else:
                    if self.filtered_data:

                        self.filtered_data = sorted(self.filtered_data,
                                                    key=lambda x:
                                                    x[column],
                                                    reverse=False)


                    else:
                        self.filtered_data = sorted(list_reader,
                                                    key=lambda x:
                                                    x[column],
                                                    reverse=False)

            else:
                raise ValueError(
                    "Недопустимая функция сортировки. Используйте 'asc' для"
                    " сортировки по возрастанию или 'desc' для сортировки по убыванию.")


        else:
            raise ValueError("Неверный оператор сортировки: используйте '='.")

    def aggregate_data(self, list_reader: list[dict],
                       aggregate: str) -> None:

        """ Агрегация"""

        if '=' in aggregate:

            # разбиение входящих данных (условия агрегации) на две части
            column, value = self.parse_condition(aggregate, '=')

            # Данные для проверки, является ли значение
            # в условии сортировки числовым значением
            data = list_reader[0]

            # проверка можно ли преобразовать строку в число
            try:
                float(data[column])
            except (ValueError, TypeError):
                raise ValueError("Агрегация требует числовых значений.")

            if value == 'avg':

                # при наличии фильтрации берём для обработки отфильтрованные данные
                if self.filtered_data:

                    avg_value = sum(float(row[column]) for row in
                                    self.filtered_data) / len(
                        self.filtered_data)
                    self.filtered_data.append({value: avg_value})

                else:

                    # если нет отфильтрованных берем данные из файла
                    avg_value = sum(
                        float(row[column]) for row in
                        list_reader) / len(
                        list_reader)
                    self.filtered_data.append({value: avg_value})

            elif value == 'max':
                # при наличии фильтрации берём для обработки отфильтрованные данные
                if self.filtered_data:
                    max_value = max(
                        float(row[column]) for row in
                        self.filtered_data)
                    self.filtered_data.append({value: max_value})
                else:
                    # если нет отфильтрованных берем данные из файла
                    max_value = max(
                        float(row[column]) for row in list_reader)
                    self.filtered_data.append({value: max_value})

            elif value == 'min':
                # при наличии фильтрации берём для обработки отфильтрованные данные
                if self.filtered_data:
                    min_value = min(
                        float(row[column]) for row in
                        self.filtered_data)
                    self.filtered_data.append({value: min_value})
                else:

                    # если нет отфильтрованных берем данные из файла
                    min_value = min(
                        float(row[column]) for row in list_reader)
                    self.filtered_data.append({value: min_value})
            else:
                raise ValueError(
                    "Недопустимое название функции агрегации."
                    " Допустимые значения: avg, min, max.")
        else:
            raise ValueError("Неверный оператор агрегации: используйте '='.")


if __name__ == '__main__':
    values_processor = FileValuesProcessor()

    file = 'products.csv'

    parser = argparse.ArgumentParser(
        description="Фильтрация и агрегация данных products.csv файла")

    # передаем именованные параметры
    parser.add_argument("--file", help="Путь файла csv")
    parser.add_argument("--where", help="Данные для фильтрации")
    parser.add_argument("--aggregate", help="Данные для агрегации")
    parser.add_argument("--order_by", help="Данные для сортировки")

    args = parser.parse_args()

    try:

        values_processor.read_file_csv(args.file, args.where, args.aggregate,
                                       args.order_by)

        if args.aggregate:
            # если выполняется агрегация, используем только данные,
            # относящиеся к результатам агрегации

            data = [values_processor.filtered_data[-1]]

            print(tabulate(data, headers="keys", tablefmt="grid"))

        elif values_processor.filtered_data:

            print(tabulate(values_processor.filtered_data, headers="keys",

                           tablefmt="grid"))





    except ValueError as e:
        print(e)
    except KeyError as e:
        print(
            f"Не найден столбец '{e}'. Убедитесь, что название столбца указано "
            f"верно и присутствует в файле.")

    except FileNotFoundError as e:
        print(
            f"Файл не найден по пути '{e}'. Проверьте правильность указанного пути.")
