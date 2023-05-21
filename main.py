import json
import pandas as pd
import matplotlib.pyplot as plt
import hashlib
from datetime import datetime

with open('DataEngineeringQ2.json') as file:
    data=json.load(file)

aggregated_data = {
    'Age': [],
    'gender': [],
    'validPhoneNumbers': [],
    'appointments': [],
    'medicines': [],
    'activeMedicines': []
}

# Create empty lists to store the extracted data
appointment_ids = []
full_names = []
phone_numbers = []
is_valid_mobiles = []
phone_number_hashes = []
genders = []
dob_list = []
ages = []
no_of_medicines_list = []
no_of_active_medicines_list = []
no_of_inactive_medicines_list = []
medicine_names_list = []


# Extract the required columns
selected_data = []
for item in data:
    appointment_id = item.get('appointmentId')
    phone_number = item.get('phoneNumber')
    patient_details = item.get('patientDetails')
    first_name = patient_details.get('firstName') if patient_details else None
    last_name = patient_details.get('lastName') if patient_details else None
    gender = patient_details.get('gender') if patient_details else None
    birth_date = patient_details.get('birthDate') if patient_details else None
    consultation_data = item.get('consultationData')
    medicines = consultation_data.get('medicines') if consultation_data else None


    # Transform gender column data
    if gender == 'M':
        gender = 'Male'
    elif gender == 'F':
        gender = 'Female'
    else:
        gender = 'Others'


    # Create the fullName column
    full_name = f"{first_name} {last_name}" if first_name or last_name else None


    #Check the validity of phone number
    def is_valid_phone_number(number):
        if number.startswith('+91') or number.startswith('91'):
            number = number[3:] if number.startswith('+91') else number[2:]
        if number.isdigit() and 6000000000 <= int(number) <= 9999999999:
            return True
        return False
    
    # Add isValidMobile column
    is_valid_mobile = is_valid_phone_number(phone_number)


    # Calculate SHA256 hash for valid phone numbers
    def calculate_phone_number_hash(number):
        if is_valid_phone_number(number):
            hash_object = hashlib.sha256()
            hash_object.update(number.encode())
            return hash_object.hexdigest()
        return None

    # Add phoneNumberHash column
    phone_number_hash = calculate_phone_number_hash(phone_number)


    # Calculate age based on DOB
    def calculate_age(dob):
        if dob is not None:
            dob = datetime.strptime(dob, "%Y-%m-%dT%H:%M:%S.%fZ")
            today = datetime.today()
            age = today.year - dob.year
            if today.month < dob.month or (today.month == dob.month and today.day < dob.day):
                age -= 1
            return age
        return None

    # Add Age column
    age = calculate_age(birth_date)


    # Count the total number of medicines
    no_of_medicines = len(medicines) if medicines else 0

    # Count the number of active and inactive medicines
    no_of_active_medicines = 0
    no_of_inactive_medicines = 0
    active_medicine_names = []  
    if medicines:
        for medicine in medicines:
            is_active = medicine.get('IsActive')
            medicine_name = medicine.get('Name')
            if is_active:
                no_of_active_medicines += 1
                active_medicine_names.append(medicine_name)
            else:
                no_of_inactive_medicines += 1


    # Append data to lists
    appointment_ids.append(appointment_id)
    full_names.append(full_name)
    phone_numbers.append(phone_number)
    is_valid_mobiles.append(is_valid_phone_number(phone_number))
    phone_number_hashes.append(phone_number_hash)
    genders.append(gender)
    dob_list.append(birth_date)
    ages.append(age)
    no_of_medicines_list.append(no_of_medicines)
    no_of_active_medicines_list.append(no_of_active_medicines)
    no_of_inactive_medicines_list.append(no_of_inactive_medicines)
    medicine_names_list.append(', '.join(active_medicine_names))


# Create a dictionary with the extracted data
data_dict = {
    'appointmentId': appointment_ids,
    'fullName': full_names,
    'phoneNumber': phone_numbers,
    'isValidMobile': is_valid_mobiles,
    'phoneNumberHash': phone_number_hashes,
    'gender': genders,
    'DOB': dob_list,
    'Age': ages,
    'noOfMedicines': no_of_medicines_list,
    'noOfActiveMedicines': no_of_active_medicines_list,
    'noOfInActiveMedicines': no_of_inactive_medicines_list,
    'MedicineNames': medicine_names_list
}

# Create a DataFrame from the dictionary
df = pd.DataFrame(data_dict)

# Export the DataFrame to a CSV file
df.to_csv('output.csv', sep='~', index=False)


# Iterate over the aggregated data
for appointment_id, values in aggregated_data.items():
    # Extract the required values from the aggregated data
    age = values['Age']
    gender = values['gender']
    valid_phone_numbers = values['validPhoneNumbers']
    appointments = values['appointments']
    medicines = values['medicines']
    active_medicines = values['activeMedicines']

    # Append the values to the respective lists in the aggregated data
    aggregated_data['Age'].append(age)
    aggregated_data['gender'].append(gender)
    aggregated_data['validPhoneNumbers'].append(valid_phone_numbers)
    aggregated_data['appointments'].append(appointments)
    aggregated_data['medicines'].append(medicines)
    aggregated_data['activeMedicines'].append(active_medicines)

# Export the aggregated data to a JSON file
with open('aggregated_data.json', 'w') as file:
    json.dump(aggregated_data, file)