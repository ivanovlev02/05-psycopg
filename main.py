import psycopg2
from pprint import pprint


def create_db(conn):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS clients(
        id SERIAL PRIMARY KEY,
        name VARCHAR(20),
        lastname VARCHAR(30),
        email VARCHAR(254)
        );
    """)
    conn.execute("""
    CREATE TABLE IF NOT EXISTS phonenumbers(
        number VARCHAR(11) PRIMARY KEY,
        client_id INTEGER REFERENCES clients(id)
        );
    """)
    return


def delete_db(conn):
    conn.execute("""
        DROP TABLE clients, phonenumbers CASCADE;
        """)


def insert_tel(conn, client_id, tel):
    conn.execute("""
        INSERT INTO phonenumbers(number, client_id)
        VALUES (%s, %s)
        """, (tel, client_id))
    return client_id


def insert_client(conn, name=None, surname=None, email=None, tel=None):
    conn.execute("""
        INSERT INTO clients(name, lastname, email)
        VALUES (%s, %s, %s)
        """, (name, surname, email))
    conn.execute("""
        SELECT id from clients
        ORDER BY id DESC
        LIMIT 1
        """)
    id = conn.fetchone()[0]
    if tel is None:
        return id
    else:
        insert_tel(conn, id, tel)
        return id


def update_client(conn, id, name=None, surname=None, email=None):
    conn.execute("""
        SELECT * from clients
        WHERE id = %s
        """, (id, ))
    info = conn.fetchone()
    if name is None:
        name = info[1]
    if surname is None:
        surname = info[2]
    if email is None:
        email = info[3]
    conn.execute("""
        UPDATE clients
        SET name = %s, lastname = %s, email =%s 
        where id = %s
        """, (name, surname, email, id))
    return id


def delete_phone(conn, number):
    conn.execute("""
        DELETE FROM phonenumbers 
        WHERE number = %s
        """, (number, ))
    return number


def delete_client(conn, id):
    conn.execute("""
        DELETE FROM phonenumbers
        WHERE client_id = %s
        """, (id, ))
    conn.execute("""
        DELETE FROM clients 
        WHERE id = %s
       """, (id,))
    return id


def find_client(conn, name=None, surname=None, email=None, tel=None):
    if name is None:
        name = '%'
    else:
        name = '%' + name + '%'
    if surname is None:
        surname = '%'
    else:
        surname = '%' + surname + '%'
    if email is None:
        email = '%'
    else:
        email = '%' + email + '%'
    if tel is None:
        conn.execute("""
            SELECT c.id, c.name, c.lastname, c.email, p.number FROM clients c
            LEFT JOIN phonenumbers p ON c.id = p.client_id
            WHERE c.name LIKE %s AND c.lastname LIKE %s
            AND c.email LIKE %s
            """, (name, surname, email))
    else:
        conn.execute("""
            SELECT c.id, c.name, c.lastname, c.email, p.number FROM clients c
            LEFT JOIN phonenumbers p ON c.id = p.client_id
            WHERE c.name LIKE %s AND c.lastname LIKE %s
            AND c.email LIKE %s AND p.number like %s
            """, (name, surname, email, tel))
    return conn.fetchall()


if __name__ == '__main__':
    with psycopg2.connect(database="netology", user="postgres", password="postgres") as conn:
        with conn.cursor() as curs:
            # Удаление таблиц перед запуском
            delete_db(curs)
            # 1. Cоздание таблиц
            create_db(curs)
            print("БД создана")
            # 2. Добавляем 5 клиентов
            print("Добавляем клиента с id: ", insert_client(curs, "Иван", "Иванов", "i.ivanov@mail.ru"))
            print("Добавляем клиента с id: ", insert_client(curs, "Артем", "Лысов", "artem321@yandex.ru", 79993318644))
            print("Добавляем клиента с id: ", insert_client(curs, "Алексей", "Бориславский", "A43.boris1@outlook.com", 79933314644))
            print("Добавляем клиента с id: ", insert_client(curs, "Роман", "Авхимович", "roma_asd2@gmail.com", 79913312643))
            print("Добавлена клиент id: ", insert_client(curs, "Ольга", "Попова", "olechka1992@yandex.ru"))
            print("Данные в таблицах")
            curs.execute("""
                SELECT c.id, c.name, c.lastname, c.email, p.number FROM clients c
                LEFT JOIN phonenumbers p ON c.id = p.client_id
                ORDER by c.id
                """)
            pprint(curs.fetchall())
            # 3. Добавляем клиенту номер телефона(одному первый, одному второй)
            print("Телефон добавлен клиенту id: ",
                  insert_tel(curs, 2, 79877876543))
            print("Телефон добавлен клиенту id: ",
                  insert_tel(curs, 1, 79621994802))

            print("Данные в таблицах")
            curs.execute("""
                SELECT c.id, c.name, c.lastname, c.email, p.number FROM clients c
                LEFT JOIN phonenumbers p ON c.id = p.client_id
                ORDER by c.id
                """)
            pprint(curs.fetchall())
            # 4. Изменим данные клиента
            print("Изменены данные клиента id: ",
                  update_client(curs, 4, "Иван", None, '123@outlook.com'))
            # 5. Удаляем клиенту номер телефона
            print("Телефон удалён c номером: ",
                  delete_phone(curs, '79621994802'))
            print("Данные в таблицах")
            curs.execute("""
                SELECT c.id, c.name, c.lastname, c.email, p.number FROM clients c
                LEFT JOIN phonenumbers p ON c.id = p.client_id
                ORDER by c.id
                """)
            pprint(curs.fetchall())
            # 6. Удалим клиента номер 2
            print("Клиент удалён с id: ",
                  delete_client(curs, 2))
            curs.execute("""
                            SELECT c.id, c.name, c.lastname, c.email, p.number FROM clients c
                            LEFT JOIN phonenumbers p ON c.id = p.client_id
                            ORDER by c.id
                            """)
            pprint(curs.fetchall())
            # 7. Найдём клиента
            print('Найденный клиент по имени:')
            pprint(find_client(curs, 'Алексей'))

            print('Найденный клиент по email:')
            pprint(find_client(curs, None, None, 'A43.boris1@outlook.com'))

            print('Найденный клиент по имени, фамилии и email:')
            pprint(find_client(curs, 'Иван', 'Авхимович', '1232@outlook..com'))

            print('Найденный клиент по имени, фамилии, телефону и email:')
            pprint(find_client(curs, "Иван", "Иванов", "i.ivanov@mail.ru", '79621994802'))

            print('Найденный клиент по имени, фамилии, телефону:')
            pprint(find_client(curs, None, None, None, '79877876543'))
