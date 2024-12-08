import psycopg2
from psycopg2 import IntegrityError


def create_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Clients (
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(40) NOT NULL,
                last_name VARCHAR(40) NOT NULL,
                email VARCHAR(40) NOT NULL        
            );
            CREATE TABLE IF NOT EXISTS phones (
                id SERIAL PRIMARY KEY,
                client_id INTEGER REFERENCES Clients(id) ON DELETE CASCADE,
                phone VARCHAR(12) UNIQUE
            );
        """)
        print('База успешно данных создана')


def is_valid_phone(phone):
    phone = phone.strip()
    if phone.startswith('+'):
        return len(phone) == 12 and phone[1:].isdigit()
    elif len(phone) == 11 and phone.isdigit():
        return True
    else:
        return False


def is_valid_email(email):
    return '@' in email and '.' in email.split('@')[-1]


def add_client(conn, first_name, last_name, email, phones=None):
    if not is_valid_email(email):
        print(f"Введеный E-mail: {email} некорректен")
        return

    if phones:
        for phone in phones:
            if not is_valid_phone(phone):
                print(f"Введеный номер телефона: {phone} некорректен")
                return

    try:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO clients (first_name, last_name, email) 
                           VALUES (%s, %s, %s) RETURNING id;""", (first_name, last_name, email))
            client_id = cur.fetchone()[0]

            if phones:
                for phone in phones:
                    add_phone(conn, client_id, phone)
            print(f"Клиент добавлен №: {client_id}")

    except Exception as error:
        print(f"Клиент не добавлен. Возникла ошибка: {error}")


def add_phone(conn, client_id, phone):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM phones WHERE client_id = %s AND phone = %s;", (client_id, phone))
            if cur.fetchone() is not None:
                print(f"Телефон {phone} уже существует, клиент № {client_id}")
                return
            cur.execute("INSERT INTO phones (client_id, phone) VALUES (%s, %s);", (client_id, phone))
            print(f"Телефон {phone} добавлен для клиента № {client_id}")
    except Exception as error:
        print(f"Ошибка при добавлении телефона: {error}")


def update_client (conn, client_id, new_first_name=None, new_last_name=None, new_email=None, new_phone=None):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM clients WHERE id = %s;", (client_id,))
            client = cur.fetchone()
            if not client:
                print(f"Клиент {client_id} не найден")
                return

            if new_first_name:
                cur.execute("UPDATE clients SET first_name = %s WHERE id = %s;", (new_first_name, client_id))

            if new_last_name:
                cur.execute("UPDATE clients SET last_name = %s WHERE id = %s;", (new_last_name, client_id))

            if new_email:
                cur.execute("UPDATE clients SET email = %s WHERE id = %s;", (new_email, client_id))

            if new_phone is not None:
                add_phone(conn, client_id, phone=new_phone)

            print(f"Данные о клиенте {client_id} изменены.")
    except Exception as e:
        print(f"Ошибка изменения данных клиента: {e}")


def delete_phone(conn, client_id, phone):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM phones WHERE client_id = %s AND phone = %s;", (client_id, phone))
            if cur.fetchone() is None:
                print(f"Телефон {phone} не найден у клиента № {client_id}")
                return
            cur.execute("DELETE FROM phones WHERE client_id = %s AND phone = %s;", (client_id, phone))
            print(f"Телефон {phone} удалён у клиента № {client_id}")
    except Exception as e:
        print(f"Ошибка удаления телефона: {e}")


def delete_client(conn, client_id):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM clients WHERE id = %s;", (client_id,))
            if cur.fetchone() is None:
                print(f"Клиент № {client_id} не найден.")
                return

            cur.execute("DELETE FROM phones WHERE client_id = %s;", (client_id,))
            cur.execute("DELETE FROM clients WHERE id = %s;", (client_id,))
            print(f"Клиент № {client_id} удалён.")
    except Exception as error:
        print(f"Ошибка удаления клиента: {error}")


def find_client(conn, first_name='%', last_name='%', email='%', phone='%'):
    try:
        with conn.cursor() as cur:
            query = """
                SELECT c.id, c.first_name, c.last_name, c.email, array_agg(p.phone) AS phones
                FROM clients c
                LEFT JOIN phones p ON p.client_id = c.id
                WHERE c.first_name ILIKE %s
                AND c.last_name ILIKE %s
                AND c.email ILIKE %s
                AND p.phone ILIKE %s
                GROUP BY c.id
            """
            params = [first_name, last_name, email, phone]

            cur.execute(query, params)
            results = cur.fetchall()

            if results:
                for row in results:
                    phones = ', '.join(filter(None, row[4]))
                    print("ID: {}, Имя: {}, Фамилия: {}, Email: {}, Телефоны: {}".format(
                        row[0], row[1], row[2], row[3], phones))
            else:
                print("Клиент не найден.")
    except Exception as e:
        print(f"Ошибка поиска клиента: {e}")


def clear_database(conn):
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS phones CASCADE;")
        cur.execute("DROP TABLE IF EXISTS clients CASCADE;")
        print("База данных очищена.")


def show_table_data(conn, table_name):
    try:
        with conn.cursor() as cur:
            query = f"SELECT * FROM {table_name};"
            cur.execute(query)
            results = cur.fetchall()
            columns = [desc[0] for desc in cur.description]

            print(f"\nДанные из таблицы {table_name}:")
            print("-" * 50)
            print(f"{' | '.join(columns)}")

            for row in results:
                print(f"{' | '.join(map(str, row))}")

            print("-" * 50)

    except Exception as e:
        print(f"Ошибка при получении данных из {table_name}: {e}")


if __name__ == "__main__":
    with psycopg2.connect(database='netology_db', user='postgres', password='Qwh912dimas') as conn:
        create_db(conn)
        add_client(conn, "Иван", "Попов", "popov_the_best@mail.ru", phones=["89130005566", "+79131115544"])
        find_client(conn, first_name="Иван")
        update_client(conn, 1, new_first_name="Петька", new_last_name="Васильев", new_email="petka_not_bad@mail.ru.ru", new_phone="89132225588")
        add_phone(conn, 1, "89133335599")
        find_client(conn, first_name="Петька")
        delete_phone(conn, 1, "89133335599")
        find_client(conn, first_name="Петька")
        delete_client(conn, 1)
        find_client(conn, first_name="Петька")
        show_table_data(conn, "clients")
        show_table_data(conn, "phones")
        clear_database(conn)
conn.close()