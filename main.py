import csv
import psycopg2


def csv_read(path: str = "./origin_db/main_animals.csv") -> list:
    """
    Считывает из csv и записывает в list
    :param path: путь до файла
    :return: результирующий список
    """
    res_list = []
    with open(path, newline="") as file:
        reader = csv.DictReader(file)
        for row in reader:
            res_list.append(row)
    return res_list


def connect():
    """
    Подключение к БД
    """
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="12321",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="animals")
        connection.autocommit = True
        cursor = connection.cursor()
    except (Exception, psycopg2.Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    return connection, cursor


def execute(cursor, req: str):
    """
    Выполнятор sql запросов
    :param req: запрос
    """
    try:
        cursor.execute(req)
    except (Exception, psycopg2.Error) as error:
        print(f"Ошибка при работе с PostgreSQL ({req[:50]})", error)


def add_to_set(set_: set, obj):
    """
    Добавляет в множество только непустые элементы
    """
    if obj:
        set_.add(obj.strip())


def split_data(csv_list: list):
    """
    Разбивает основной список с данными на маленькие и записывает нужные в словарь с кортежами
    :param csv_list: Основной список
    :return: Словарь кортежей
    """
    colors = set()
    breeds = set()
    types = set()
    outcome_types = set()
    outcome_subtypes = set()
    for row in csv_list:
        add_to_set(colors, row["color1"])
        add_to_set(colors, row["color2"])
        add_to_set(breeds, row["breed"])
        add_to_set(types, row["animal_type"])
        add_to_set(outcome_types, row["outcome_type"])
        add_to_set(outcome_subtypes, row["outcome_subtype"])
    return {"colors": tuple(colors), "breeds": tuple(breeds), "types": tuple(types), "outcome_types": tuple(outcome_types), "outcome_subtypes": tuple(outcome_subtypes)}


def split_complex_data(csv_list: list, csv_dict: dict):
    """
    Разделяет основной список на два больших
    :param csv_list: Основной список
    :param csv_dict: Словарь для записи результата
    """
    animals = []
    shelter = []
    for row in csv_list:
        animals.append((row["animal_id"], row["animal_type"], row["name"], row["breed"], row["color1"], row["color2"], row["date_of_birth"]))
        shelter.append((row["index"], row["animal_id"], row["outcome_subtype"], row["outcome_month"], row["outcome_year"], row["outcome_type"], row["age_upon_outcome"]))
    csv_dict["animals"] = tuple(set(animals))
    csv_dict["shelter"] = tuple(shelter)


def format_tuple(start_tuple: tuple) -> tuple:
    """
    Форматирует кортеж под sql запрос
    :param start_tuple: Кортеж для форматирования
    :return: Результирующий кортеж
    """
    list_ = []
    for i in range(len(start_tuple)):
        list_.append((i + 1, start_tuple[i]))
    return tuple(list_)


def redact_list(origin_list: list, csv_dict: dict):
    """
    Заменяет в списке значения на индексы и форматирует некоторые значения под sql запрос
    :param origin_list: Список данных
    :param csv_dict: Словарь с кортежами для получения индексов
    """
    for row in origin_list:
        row["animal_type"] = csv_dict["types"].index(row["animal_type"].strip()) + 1
        row["breed"] = csv_dict["breeds"].index(row["breed"].strip()) + 1
        row["color1"] = csv_dict["colors"].index(row["color1"].strip()) + 1
        if row["color2"]:
            row["color2"] = csv_dict["colors"].index(row["color2"].strip()) + 1
        else:
            row["color2"] = "NULL"
        if row["outcome_subtype"]:
            row["outcome_subtype"] = csv_dict["outcome_subtypes"].index(row["outcome_subtype"].strip()) + 1
        else:
            row["outcome_subtype"] = "NULL"
        if row["outcome_type"]:
            row["outcome_type"] = csv_dict["outcome_types"].index(row["outcome_type"].strip()) + 1
        else:
            row["outcome_type"] = "NULL"
        row["name"] = row["name"].replace("'", "''")


def create_table(cursor, name: str, columns: str):
    """
    Отправляет запрос на создание таблицы в БД
    :param name: Название таблицы
    :param columns: Колонки
    """
    req = f"""
    CREATE TABLE IF NOT EXISTS {name}(
        {columns}
        )
    """
    execute(cursor, req)


def create_all_tables(cursor):
    """
    Создаёт все таблицы
    """
    create_table(cursor, "Type_Dict", "id INTEGER NOT NULL UNIQUE, type varchar(40)")
    create_table(cursor, "Breed_Dict", "id INTEGER NOT NULL UNIQUE, breed varchar(40)")
    create_table(cursor, "Color_Dict", "id INTEGER NOT NULL UNIQUE, color varchar(40)")
    create_table(cursor, "Outcome_types", "id INTEGER NOT NULL UNIQUE, outcome_type varchar(40)")
    create_table(cursor, "Outcome_subtypes", "id INTEGER NOT NULL UNIQUE, outcome_subtype varchar(40)")
    create_table(cursor, "Animals", """
                                    id VARCHAR(10) NOT NULL UNIQUE,
                                    type_id INTEGER NOT NULL,
                                    name VARCHAR,
                                    breed_id INTEGER NOT NULL,
                                    color1_id INTEGER NOT NULL,
                                    color2_id INTEGER,
                                    date_of_birth TIMESTAMP,
                                    FOREIGN KEY (type_id) REFERENCES type_dict (id),
                                    FOREIGN KEY (breed_id) REFERENCES breed_dict (id),
                                    FOREIGN KEY (color1_id) REFERENCES color_dict (id),
                                    FOREIGN KEY (color2_id) REFERENCES color_dict (id)
                                    """)
    create_table(cursor, "Shelter", """
                                    id VARCHAR(10) NOT NULL UNIQUE,
                                    animal_id VARCHAR(10) NOT NULL,
                                    outcome_subtype_id INTEGER,
                                    outcome_month INTEGER,
                                    outcome_year INTEGER,
                                    outcome_type_id INTEGER,
                                    age_upon_outcome VARCHAR(20),
                                    FOREIGN KEY (animal_id) REFERENCES animals (id),
                                    FOREIGN KEY (outcome_subtype_id) REFERENCES outcome_subtypes (id),
                                    FOREIGN KEY (outcome_type_id) REFERENCES outcome_types (id)
                                    """)


def insert_into(cursor, name_table: str, obj: tuple):
    """
    Добавлятор
    :param name_table: Название таблицы
    :param obj: кортеж объектов для добавления
    """
    if len(obj) == 1:
        values = f"{obj[0]}"
    else:
        values = str(obj)[1:-1].replace("'NULL'", "NULL").replace("\"", "'")
    req = f"""
          INSERT INTO {name_table}
          VALUES {values}
          """
    execute(cursor, req)


def insert_all(cursor, csv_dict: dict):
    """
    Выполняет все необходимые добавления
    :param csv_dict: Словарь данных
    """
    insert_into(cursor, "color_dict", format_tuple(csv_dict["colors"]))
    insert_into(cursor, "type_dict", format_tuple(csv_dict["types"]))
    insert_into(cursor, "breed_dict", format_tuple(csv_dict["breeds"]))
    insert_into(cursor, "outcome_subtypes", format_tuple(csv_dict["outcome_subtypes"]))
    insert_into(cursor, "outcome_types", format_tuple(csv_dict["outcome_types"]))
    insert_into(cursor, "animals", csv_dict["animals"])
    insert_into(cursor, "shelter", csv_dict["shelter"])


if __name__ == '__main__':
    csv_list = csv_read()
    csv_dict = split_data(csv_list)
    redact_list(csv_list, csv_dict)
    split_complex_data(csv_list, csv_dict)

    connection, cursor = connect()
    create_all_tables(cursor)
    insert_all(cursor, csv_dict)
    cursor.close()
    connection.close()

