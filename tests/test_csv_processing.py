import csv

import pytest
from src.csv_processing import FileValuesProcessor


class TestCsvProcessing:

    @pytest.fixture
    def csv_data(self):
        file_path = 'tests/test.csv'
        with open(file_path, mode='r', encoding='utf-8',
                  newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            data = list(reader)
        return data

    def test_csv_content(self, csv_data: list[dict[str, str]]):
        """ Тест содержимого файла"""
        assert isinstance(csv_data, list)
        assert all(isinstance(row, dict) for row in csv_data)
        assert 'name' in csv_data[0]

    def test_open_csv_invalid(self, csv_data: list[dict[str, str]]):
        """ Тест ошибки открытия файла"""
        with pytest.raises(FileNotFoundError):
            file_path = 'test.csv'
            with open(file_path, mode='r', encoding='utf-8',
                      newline='') as csvfile:
                reader = csv.DictReader(csvfile)

    @pytest.mark.parametrize(
        "filter_param, expected",
        [
            ("name=Civic", [
                {"name": "Civic", "brand": "Honda", "price": "24999",
                 "rating": "4.5"}
            ]),
            ("price>10000", [
                {"name": "Model S", "brand": "Tesla", "price": "89999",
                 "rating": "4.8"},
                {"name": "Mustang", "brand": "Ford", "price": "55999",
                 "rating": "4.6"},
                {"name": "Civic", "brand": "Honda", "price": "24999",
                 "rating": "4.5"},
                {"name": "Camry", "brand": "Toyota", "price": "27999",
                 "rating": "4.4"},
                {"name": "A4", "brand": "Audi", "price": "39999",
                 "rating": "4.3"},
            ]),
            ("rating<5", [
                {"name": "Model S", "brand": "Tesla", "price": "89999",
                 "rating": "4.8"},
                {"name": "Mustang", "brand": "Ford", "price": "55999",
                 "rating": "4.6"},
                {"name": "Civic", "brand": "Honda", "price": "24999",
                 "rating": "4.5"},
                {"name": "Camry", "brand": "Toyota", "price": "27999",
                 "rating": "4.4"},
                {"name": "A4", "brand": "Audi", "price": "39999",
                 "rating": "4.3"},
            ]),
            ("brand=Ford", [
                {"name": "Mustang", "brand": "Ford", "price": "55999",
                 "rating": "4.6"},
            ]),
        ]
    )
    def test_filter_data(self,
                         csv_data: list,
                         filter_param: str,
                         expected: list[dict[str, str]]):
        """ Тест фильтрации данных файла"""
        filter_processor = FileValuesProcessor()
        list_data = filter_processor.filter_data(csv_data, filter_param)
        assert isinstance(filter_processor.filtered_data, list)
        assert filter_processor.filtered_data == expected

    def test_filter_data_invalid_filter(self):
        """ Тест ошибки фильтрации данных файла"""
        filter_processor = FileValuesProcessor()
        data = [{"brand": "Ford"}]

        with pytest.raises(ValueError):
            filter_processor.filter_data(data,
                                         "bran-Ford")  # опечатка в колонке

        with pytest.raises(ValueError):
            filter_processor.filter_data(data,
                                         "brand=unknown_func")  # неправильная функция

    @pytest.mark.parametrize("aggregate_param, expected", [
        ("price=avg", {'avg': 47799.0}),
        ("rating=min", {'min': 4.3}),
        ("rating=avg", {'avg': 4.52}),
        ("price=max", {'max': 89999}),

    ])
    def test_aggregate_data(self,
                            csv_data: list[dict[str, str]],
                            aggregate_param: str,
                            expected: list[dict[str, str]]):
        """ Тест агрегации данных файла"""
        aggregate_processor = FileValuesProcessor()
        aggregate_processor.aggregate_data(csv_data,
                                           aggregate_param)

        assert aggregate_processor.filtered_data[-1] == expected

        assert len(aggregate_processor.filtered_data) > 0

        assert isinstance(aggregate_processor.filtered_data[-1], dict)

    def test_aggregate_data_invalid_aggregate(self):
        """ Тест ошибки агрегации данных файла"""
        aggregate_processor = FileValuesProcessor()
        data = [{"price": "100"}]  # правильный тип данных

        with pytest.raises(ValueError):
            aggregate_processor.aggregate_data(data,
                                               "price-avg")  # опечатка в колонке

        with pytest.raises(ValueError):
            aggregate_processor.aggregate_data(data,
                                               "price=unknown_func")  # неправильная функция

    @pytest.mark.parametrize("order_by_param, expected", [
        (
                "price=desc",
                [
                    {"name": "Model S", "brand": "Tesla", "price": "89999",
                     "rating": "4.8"},
                    {"name": "Mustang", "brand": "Ford", "price": "55999",
                     "rating": "4.6"},
                    {"name": "A4", "brand": "Audi", "price": "39999",
                     "rating": "4.3"},
                    {"name": "Camry", "brand": "Toyota", "price": "27999",
                     "rating": "4.4"},
                    {"name": "Civic", "brand": "Honda", "price": "24999",
                     "rating": "4.5"},
                ]
        ),
        (
                "brand=asc",
                [
                    {"name": "A4", "brand": "Audi", "price": "39999",
                     "rating": "4.3"},
                    {"name": "Mustang", "brand": "Ford", "price": "55999",
                     "rating": "4.6"},
                    {"name": "Civic", "brand": "Honda", "price": "24999",
                     "rating": "4.5"},
                    {"name": "Model S", "brand": "Tesla", "price": "89999",
                     "rating": "4.8"},
                    {"name": "Camry", "brand": "Toyota", "price": "27999",
                     "rating": "4.4"},
                ]
        )
    ])
    def test_order_by_data(self,
                           csv_data: list[dict[str, str]],
                           order_by_param: str,
                           expected: list[dict[str, str]]):
        """ Тест ошибки сортировки данных файла"""

        order_by_processor = FileValuesProcessor()
        order_by_processor.order_by_data(csv_data, order_by_param)

        assert isinstance(order_by_processor.filtered_data, list)
        assert order_by_processor.filtered_data == expected

    def test_order_by_data_invalid_order_by(self):
        """ Тест ошибки сортировки данных файла"""
        aggregate_processor = FileValuesProcessor()

        data = [{"price": "100"}]  # правильный тип данных

        with pytest.raises(ValueError):
            aggregate_processor.order_by_data(data,
                                              "price-desc")  # опечатка в колонке

        with pytest.raises(ValueError):
            aggregate_processor.order_by_data(data,
                                              "price=unknown_func")  # неправильная функция

    def test_filter_data_no_matching_records(self,
                                             csv_data: list[dict[str, str]]):
        """Тест фильтрации данных файла, проверяющий поведение при отсутствии
         записей, соответствующих условиям фильтрации."""
        processor = FileValuesProcessor()
        with pytest.raises(ValueError) as exc_info:
            processor.filter_data(csv_data,
                                  "price>1000000")
        assert "Не найдено записей, соответствующих условиям фильтрации" in str(
            exc_info.value)

    @pytest.mark.parametrize("data_operations,operator, expected", [
        ("price=avg", "=",(('price','avg'))),
        ("rating=desc","=", (('rating','desc'))),
        ("rating>1000", ">",(('rating','1000'))),
        ("price=min", "=",(('price','min'))),

    ])
    def test_parse_condition(self,data_operations,operator,expected):
        """ Тест разбиение строки на две части"""
        processor = FileValuesProcessor()
        result = processor.parse_condition(data_operations,operator)
        assert result == expected
        assert isinstance(result,tuple)
