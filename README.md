# test_job
Файл test_job.py содержит:
1. Класс для работы с БД - Database
    Содержит конструктор, который подключается к 'calls-for-service.db', методы создания таблиц, методы insert и delete.
    Так же в нем реализована функция поиска записей (select_from_table_calls) по вхождению в заданные даты и параметр page для возможности ограничивать кол-во результатов по 20 шт. на страницу.
    При отсутствии параметра при вызове возвращает все полученные на запрос данные.
    Результатом выполнения является словарь вида rez = { 'total_records' : 0, 'records' : [] }, который содержит общее количество полученных записей и сами записи с учетом параметра page.
    "Try:except:" сделаны только в select_from_table_calls. Данные примера статичны и сильно нагружать исключениями код не хотел.
2. Функцию search_for_mini_table, которая создает и заполняет связанные таблицы
3. Фунцию load_to_db(int), которая загружает данные в базу из CSV. Параметр позволяет задавать ограничение по количеству копируемых строк.
  При отсутствии параметра при вызове копирует все строки из файла.
  
  
Функции search_for_mini_table и load_to_db обернуты в декоратор, калькулирующий время выполнения.
Логироние выполнено через logging в файл test_job.log
