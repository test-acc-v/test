from database.database import (
    Database,
    DepartmentTable,
    EmployeeLeaveTable,
    EmployeeTable,
)

if __name__ == "__main__":
    db = Database()

    # Создание таблиц в базе данных
    db.register_table("employees", EmployeeTable())
    db.register_table("departments", DepartmentTable())
    db.register_table("employees_leaves", EmployeeLeaveTable())

    # Вставка элементов
    db.insert("employees", "1,Alice,30,70000,1")
    db.insert("employees", "2,Bob,29,100000,1")
    db.insert("departments", "1,Engineering")
    db.insert("employees_leaves", "1,1,01.06.2025,01.07.2025")
    db.insert("employees_leaves", "2,2,10.09.2025,24.09.2025")
