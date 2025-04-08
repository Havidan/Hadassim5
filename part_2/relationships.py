import sqlite3
import random
import csv
from faker import Faker

import sqlite3

def create_people_table_sqlite():
    conn = sqlite3.connect("C:\\Users\\This_user\\Desktop\\לימודים\\Hadasim\\part_2\\family_tree.db")
    cursor = conn.cursor()
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='People';")
    result = cursor.fetchone()
    
    if not result:
        print("Creating People table...")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS People (
            Person_Id INTEGER PRIMARY KEY,
            Personal_Name TEXT,
            Family_Name TEXT,
            Gender TEXT,
            Father_Id INTEGER,
            Mother_Id INTEGER,
            Spouse_Id INTEGER,
            FOREIGN KEY (Father_Id) REFERENCES People (Person_Id),
            FOREIGN KEY (Mother_Id) REFERENCES People (Person_Id),
            FOREIGN KEY (Spouse_Id) REFERENCES People (Person_Id)
        )""")
        conn.commit()
        
        populate_people_sqlite(conn, cursor)
    else:
        print("People table already exists, skipping creation.")
    
    conn.close()



def populate_people_sqlite(conn, cursor):
    fake = Faker()
    genders = ['M', 'F']
    
    people = []
    for id in range(10):
        name = fake.first_name()
        family = fake.last_name()
        gender = random.choice(genders)
        people.append({'id': id, 'name': name, 'family': family, 'gender': gender, 'father': None, 'mother': None, 'spouse': None})
        
    spouses = set()
    for person in people:
        possible_fathers = list(filter(lambda p: p['gender'] == "M" and p['id'] != person['id'], people))
        possible_mothers = list(filter(lambda p: p['gender'] == "F" and p['id'] != person['id'], people))

        if possible_fathers and possible_mothers:
            father = random.choice(possible_fathers)
            mother = random.choice(possible_mothers)
            person['father'] = father['id']
            person['mother'] = mother['id']

        spouse=None
        if random.random() > 0.4:
            if person['id'] not in spouses:     
                potential_spouses = list(filter(lambda p: p['id'] not in spouses and p['id'] != person['id'] and p['id'] != person['father'] and p['id'] != person['mother'], people))
                if potential_spouses:
                    spouse = random.choice(potential_spouses)
                    spouses.add(person['id'])
                    spouses.add(spouse['id'])
                    person['spouse'] = spouse['id']
                    if random.random() < 0.8:
                        spouse['spouse'] = person['id']
                    
    for person in people:
        cursor.execute("INSERT INTO People (Person_Id,Personal_Name, Family_Name, Gender, Father_Id, Mother_Id, Spouse_Id) VALUES (?,?, ?, ?, ?, ?,?)", 
                       (person['id'], person['name'], person['family'], person['gender'], person['father'], person['mother'], person['spouse']))
    
    conn.commit()
    cursor.execute("""SELECT * FROM People""")
    print(cursor.fetchall())


def find_relationships():
    conn = sqlite3.connect("C:\\Users\\This_user\\Desktop\\לימודים\\Hadasim\\part_2\\family_tree.db")
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM People")
    people = cursor.fetchall()

    relationships = []
    for person in people:
        person_id, _, _, gender, father_id, mother_id, spouse_id = person
        
        if father_id is not None:
            relationships.append((person_id, father_id, 'Father'))  
            if gender == "M":
                relationships.append((father_id, person_id, 'Son'))
            else:
                relationships.append((father_id, person_id, 'Daughter')) 

        if mother_id is not None:
            relationships.append((person_id, mother_id, 'Mother'))  
            if gender == "M":
                relationships.append((mother_id, person_id, 'Son'))
            else:
                relationships.append((mother_id, person_id, 'Daughter'))

        cursor.execute("SELECT * FROM People WHERE Father_Id = ? OR Mother_Id = ?", (father_id, mother_id))
        siblings =  cursor.fetchall()

        for sibling in siblings:
            sibling_id = sibling[0]  
            sibling_gender = sibling[3]  

            if sibling_id != person_id: 
                if sibling_gender == "M":
                    relationships.append((person_id, sibling_id, 'Brother'))
                else:
                    relationships.append((person_id, sibling_id, 'Sister'))

        if spouse_id != None:
            relationships.append((person_id, spouse_id, 'Spouse'))
            cursor.execute("SELECT Spouse_Id FROM People WHERE Person_Id = ?", (spouse_id, ))
            sp=cursor.fetchall()
            if sp[0][0]==None:
                relationships.append((spouse_id, person_id, 'Spouse'))

    with open('C:\\Users\\This_user\\Desktop\\לימודים\\Hadasim\\part_2\\relationships.csv', 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['Person_Id', 'Relative_Id', 'Connection_Type'])
        csv_writer.writerows(relationships)

    conn.commit()
    conn.close()
    

if __name__ == "__main__":

    create_people_table_sqlite()
    find_relationships()
    print("SQLite Database and CSV export completed successfully!")