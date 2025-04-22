import psycopg2
import csv

connection = psycopg2.connect( # Connecting to the phonebook db
    database = "phonebook",
    user = "postgres",
    password = "Kbtu4370!",
    host = "localhost"
)

def create_table():

    command = """
            CREATE TABLE IF NOT EXISTS users(
            user_id SERIAL PRIMARY KEY,
            user_name VARCHAR(255),
            user_phone VARCHAR(20)
            )
        """
    with connection.cursor() as cur:
        # execute the command
        cur.execute(command)
    connection.commit() # save change to the db

def insert_user():

    user_name = input("Enter user name: ")
    user_phone = input("Enter user phone: ")

    command = """
        INSERT INTO users(user_name,user_phone) VALUES (%s,%s)
    """

    with connection.cursor() as cur:
        # execute the command
        cur.execute(command, (user_name,user_phone))

    connection.commit()
    print("\nuser added successfully!")

def insert_from_csv(file_path):
    try:
        with connection.cursor() as cur:
            with open(file_path,'r') as f:
                csvreader = csv.reader(f)
                next(csvreader) # skip the title
                for row in csvreader:
                    cur.execute("INSERT INTO users (user_name,user_phone) VALUES (%s,%s)",(row[0],row[1]))

            connection.commit()
            print("\nData from CSV added successfully!")
    except:
        print("\nX Invalid file entered X")

        
def update_user_phone():
    name = input("Enter user_name to update: ")
    new_phone = input("Enter new phone number: ")

    command = "UPDATE users SET user_phone = %s WHERE user_name = %s"

    with connection.cursor() as cur:
        cur.execute(command, (new_phone,name))

    connection.commit()
    print("\nContact updated!")

def update_user_name():
    name = input("Enter user_name to update: ")
    new_name = input("Enter new user_name: ")

    command = "UPDATE users SET user_name = %s WHERE user_name = %s"

    with connection.cursor() as cur:
        cur.execute(command,(new_name,name))

    connection.commit()
    print("\nContact updated!")

def insert_or_update():
    name_input = input("Enter user name: ")
    phone_input = input("Enter user phone: ")
    
    command = "CALL insert_or_update(%s,%s)"

    with connection.cursor() as cur:
        cur.execute(command,(name_input,phone_input))
    connection.commit()
    print("\nUser added or updated successfully!")

    """
        CREATE OR REPLACE PROCEDURE insert_or_update(name_input VARCHAR, phone_input VARCHAR)
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF EXISTS (SELECT 1 FROM users WHERE user_name = name_input) THEN
                UPDATE users SET user_phone = phone_input WHERE user_name = name_input;
            ELSE
                INSERT INTO users(user_name, user_phone) VALUES (name_input, phone_input);
            END IF;
        END;
        $$;

    """

def filter_by_first_letter():
    letter = input("Enter letter: ")
    command = " SELECT * FROM users WHERE user_name ILIKE %s"
  
    with connection.cursor() as cur: 
        cur.execute(command, (letter + '%',))
        results = cur.fetchall()
        for row in results:
            print(row)
    print("\nfiltering by first letter completed!")

def filter_by_part():
    keyword = input("Enter letters: ")
    command = "SELECT * FROM users WHERE user_name ILIKE %s"

    with connection.cursor() as cur:
        cur.execute(command,("%" + keyword + "%",))
        results = cur.fetchall()
        for row in results:
            print(row)

    print("\nfiltering by part completed!")

def search_by_pattern():
    pattern = input("Enter pattern (part of name or phone): ")
    command = "SELECT * FROM search_by_pattern(%s)"
    with connection.cursor() as cur:
        cur.execute(command,(pattern,))
        results = cur.fetchall()
        for row in results:
            print(row)
    print("\nSearch by pattern completed!")

    """
        CREATE OR REPLACE FUNCTION search_by_pattern(pattern VARCHAR(255))
        RETURNS TABLE(user_id INT, user_name VARCHAR(255), user_phone VARCHAR(255))
        AS $$
        BEGIN
            RETURN QUERY
            SELECT u.user_id, u.user_name, u.user_phone
            FROM users u
            WHERE u.user_name ILIKE '%' || pattern || '%'
                OR u.user_phone ILIKE '%' || pattern || '%';
        END;
        $$ LANGUAGE plpgsql;

    """

def delete_user():

    name_or_phone = input("Enter name or phone number: ")

    command = "CALL delete_user_by_name_or_phone(%s)"

    with connection.cursor() as cur:
        cur.execute(command, (name_or_phone,))

    connection.commit()
    
    print("\nContact deleted successfully!")

    """
        CREATE OR REPLACE PROCEDURE delete_user_by_name_or_phone(name_or_phone VARCHAR(255))
        LANGUAGE plpgsql
        AS $$
        BEGIN
            DELETE FROM users
            WHERE user_name = name_or_phone OR user_phone = name_or_phone;
        END;
        $$;

    """

def get_paginated_users():
    limit = int(input("Enter number of records to show: "))
    offset = int(input("Enter offset (how many row to skip): "))
    command = "SELECT * FROM get_users_with_pagination(%s,%s)"

    with connection.cursor() as cur:
        cur.execute(command,(limit,offset))
        results = cur.fetchall()
        for row in results:
            print(row)

    print("\nPagination complete!")

    """
        CREATE OR REPLACE FUNCTION get_users_with_pagination(u_limit INT, u_offset INT)
        RETURNS TABLE(user_id INT, user_name VARCHAR(255), user_phone VARCHAR(255))
        AS $$
        BEGIN
            RETURN QUERY
            SELECT u.user_id, u.user_name, u.user_phone
            FROM users u
            ORDER BY u.user_id
            LIMIT u_limit OFFSET u_offset;
        END;
        $$ LANGUAGE plpgsql;

    """

def truncate_table():
    command = "TRUNCATE TABLE users RESTART IDENTITY"

    with connection.cursor() as cur:
        cur.execute(command)

    connection.commit()

    print("\nThe table has been cleared!")

def show_all_users():
    command = "SELECT * FROM users"

    with connection.cursor() as cur:
        cur.execute(command)
        result = cur.fetchall()
        print("user_id  user_name  user_phone")
        for row in result:
            print(row[0], row[1], row[2])

    connection.commit()

if __name__ == "__main__":
    create_table() # create a table if it doesn't exist yet

    while True:
        print("\n PHONEBOOK MENU: ")
        print("1. Add new user")
        print("2. Load users from CSV")
        print("3. Update user's phone")
        print("4. Update user's name")
        print("5. Insert or update user")
        print("6. Filter by first letter")
        print("7. Filter by part")
        print("8. Search by pattern")
        print("9. Delete user")
        print("10. Pagination table ")
        print("11. Clear table")
        print("12. Show all users")
        print("0. Exit")

        choice = input("Choose option: ")

        if choice == '1':
            insert_user() 
        elif choice == '2':
            path_csv = input("Enter csv path: ")
            insert_from_csv(path_csv)
        elif choice == '3':
            update_user_phone()
        elif choice == '4':
            update_user_name()
        elif choice == '5':
            insert_or_update()
        elif choice == '6':
            filter_by_first_letter()
        elif choice == '7':
            filter_by_part()
        elif choice == '8':
            search_by_pattern()
        elif choice == '9':
            delete_user()
        elif choice == '10':
            get_paginated_users()
        elif choice == '11':
            truncate_table()
        elif choice == '12':
            show_all_users()
        elif choice == '0':
            print("\nYou have exited the PhoneBook!")
            break
        else:
            print("\nInvalid option X")