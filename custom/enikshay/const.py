# Case property names used in integrations
PRIMARY_PHONE_NUMBER = 'phone_number'
BACKUP_PHONE_NUMBER = 'secondary_contact_phone_number'
OTHER_NUMBER = "phone_number_other"
NINETYNINEDOTS_NUMBERS = [
    PRIMARY_PHONE_NUMBER,
    BACKUP_PHONE_NUMBER,
    OTHER_NUMBER,
]

MERM_ID = 'merm_id'
MERM_DAILY_REMINDER_STATUS = 'merm_daily_reminder_status'
MERM_DAILY_REMINDER_TIME = "merm_daily_reminder_time"
MERM_REFILL_REMINDER_STATUS = "merm_refill_reminder_status"
MERM_REFILL_REMINDER_DATETIME = "merm_refill_reminder_datetime"
MERM_RT_HOURS = "merm_rt_hours"
MERM_PROPERTIES = [
    MERM_ID,
    MERM_DAILY_REMINDER_STATUS,
    MERM_DAILY_REMINDER_TIME,
    MERM_REFILL_REMINDER_STATUS,
    MERM_REFILL_REMINDER_DATETIME,
    MERM_RT_HOURS,
]

ENROLLED_IN_PRIVATE = "enrolled_in_private"

ENIKSHAY_ID = 'person_id'
PERSON_FIRST_NAME = 'first_name'
PERSON_LAST_NAME = 'last_name'

TREATMENT_START_DATE = 'treatment_initiation_date'
TREATMENT_SUPPORTER_FIRST_NAME = 'treatment_supporter_first_name'
TREATMENT_SUPPORTER_LAST_NAME = 'treatment_supporter_last_name'
TREATMENT_SUPPORTER_PHONE = 'treatment_supporter_mobile_number'

TREATMENT_OUTCOME = 'treatment_outcome'
TREATMENT_OUTCOME_DATE = 'treatment_outcome_date'
EPISODE_PENDING_REGISTRATION = "episode_pending_registration"
PRIVATE_PATIENT_EPISODE_PENDING_REGISTRATION = "private_sector_episode_pending_registration"
WEIGHT_BAND = 'weight_band'
LAST_VOUCHER_CREATED_BY_ID = "bets_last_voucher_created_by_id"
NOTIFYING_PROVIDER_USER_ID = "bets_notifying_provider_user_id"

CURRENT_ADDRESS = 'current_address'
ENROLLED_IN_PRIVATE = "enrolled_in_private"

NINETYNINEDOTS_PERSON_PROPERTIES = [
    ENIKSHAY_ID,
    PRIMARY_PHONE_NUMBER,
    BACKUP_PHONE_NUMBER,
    OTHER_NUMBER,
    PERSON_FIRST_NAME,
    PERSON_LAST_NAME,
    CURRENT_ADDRESS,
    ENROLLED_IN_PRIVATE,
    'owner_id',
]
NINETYNINEDOTS_EPISODE_PROPERTIES = [
    TREATMENT_SUPPORTER_FIRST_NAME,
    TREATMENT_SUPPORTER_LAST_NAME,
    TREATMENT_SUPPORTER_PHONE,
    TREATMENT_OUTCOME,
    TREATMENT_OUTCOME_DATE,
    TREATMENT_START_DATE,
    WEIGHT_BAND,
] + MERM_PROPERTIES

DOSE_TAKEN_INDICATORS = [
    'directly_observed_dose',
    'unobserved_dose',
    'self_administered_dose',
]

DOSE_MISSED = 'missed_dose'
DOSE_UNKNOWN = 'missing_data'
DOSE_KNOWN_INDICATORS = DOSE_TAKEN_INDICATORS + [DOSE_MISSED]
DAILY_SCHEDULE_FIXTURE_NAME = 'adherence_schedules'
DAILY_SCHEDULE_ID = 'schedule_daily'
SCHEDULE_ID_FIXTURE = 'id'
# one of values of 'adherence_closure_reason' case property
HISTORICAL_CLOSURE_REASON = 'historical'
PRESCRIPTION_TOTAL_DAYS_THRESHOLD = "prescription_total_days_threshold_{}"

# Voucher Case Properties
DATE_FULFILLED = "date_fulfilled"
VOUCHER_ID = "voucher_id"
FULFILLED_BY_ID = "fulfilled_by_id"
FULFILLED_BY_LOCATION_ID = "voucher_fulfilled_by_location_id"
AMOUNT_APPROVED = "amount_approved"
INVESTIGATION_TYPE = "investigation_type"

ENIKSHAY_TIMEZONE = 'Asia/Kolkata'

AGENCY_USER_FIELDS = [
    # (slug, label, choices)
    ('tb_corner', "TB Corner", ["Yes", "No"]),
    ('pcp_qualification', "MBBS Qualification", ["MBBS", "DTCD", "MD - Chest Physician",
                                                 "MD - Medicine", "MS", "DM"]),
    ('pac_qualification', "AYUSH Qualification", ["BAMS", "BHMS", "BUMS", "DAMS", "DHMS", "ASHA",
                                                  "ANM", "GNM", "LCEH", "NGO", "Others", "None"]),
    ('pcp_professional_org_membership', "Professional Org Membership", ["IMA", "WMA", "AMA", "AAFP",
                                                                        "Others", "None"]),
    ('plc_lab_collection_center_name', "Lab/Collection Center Name", []),
    ('plc_lab_or_collection_center', "Lab or Collection Center", ["Lab", "Collection Center", "Both",
                                                                  "Government lab/DMC"]),
    ('plc_accredidation', "Lab Accredidation", ["NABL", "NABH", "BIS", "RNTCP", "Others", "None", "COPA"]),
    ('plc_tb_tests', "TB Tests", []),  # TODO Same lab option as investigations master
    ('plc_hf_if_nikshay', "HF-ID Nikshay", []),
    ('pcc_pharmacy_name', "Pharmacy Name", []),
    ('pcc_pharmacy_affiliation', "Pharmacy Affiliation", ["IPA", "AIOCD"]),
    ('pcc_tb_drugs_in_stock ', "TB Drugs in Stock", ["Private drugs only", "Goverment drugs (FDCs)",
                                                     "Private and government drugs"]),
    ('agency_status', "Status", ["Registered", "Mapped", "Mapped and Targeted", "engaged- state scheme",
                                 "dropped"]),
    ('gender', "Gender", ["Male", "Female", "Transgender"]),
    ('registration_number', "Registration Number", []),
    ('issuing_authority', "Issuing Authority", ["State Medical Council (SMC)",
                                                "Medical Council of India (MCI)"]),
    # TODO Do these ones make sense?
    # ('', "Associated FO", []),
    # ('', "Attached to Parent Agency", ["Yes", "No"]),
    # ('', "Parent Agency Name", []),
    # ('', "Training attended?", ["Yes", "No"]),
    # ('', "Training date", []),
    # ('', "Alert to Agency", ["Yes", "No"]),
    # ('', "Payments to Parent Agency", ["Yes", "No"]),

    ("address_line_1", "Address Line 1", []),
    ("address_line_2", "Address Line 2", []),
    ("pincode", "Pincode", []),
    ("mobile_no_1", "Mobile No. 1", []),
    ("mobile_no_2", "Mobile No. 2", []),
    ("landline_no", "Landline No.", []),
    ("email", "Email", []),

    # Secondary user
    ("secondary_first_name", "Secondary User First Name", []),
    ("secondary_middle_name", "Secondary User Middle Name ", []),
    ("secondary_last_name", "Secondary User Last Name", []),
    ("secondary_date_of_birth", "Secondary User Date of Birth", []),
    ('secondary_gender', "Secondary User Gender", ["Male", "Female", "Transgender"]),
    ("secondary_unique_id_type", "Secondary User Unique ID type",
     ["Aadhaar", "PAN Card", "Driving License", "Ration Card", "Voter ID", "Others"]),
    ("secondary_unique_id_Number", "Secondary User Unique ID Number", []),
    ("secondary_address_line_1", "Secondary User Address Line 1", []),
    ("secondary_address_line_2", "Secondary User Address Line 2", []),
    ("secondary_pincode", "Secondary User Pincode", []),
    ("secondary_mobile_no_1", "Secondary User Mobile No. 1", []),
    ("secondary_mobile_no_2", "Secondary User Mobile No. 2", []),
    ("secondary_landline_no", "Secondary User Landline No.", []),
    ("secondary_email", "Secondary User Email", []),
]

AGENCY_LOCATION_FIELDS = [
    ('private_sector_org_id', "Private Sector Org ID", []),
]
