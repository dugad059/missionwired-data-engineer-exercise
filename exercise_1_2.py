import pandas as pd

# EXERCISE 1

print('Loading data exercise 1...')
# Cons Table
cons = pd.read_csv('https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons.csv')

# Emails Table
cons_email = pd.read_csv('https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email.csv')

# Subs Table
subs = pd.read_csv('https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email_chapter_subscription.csv')
print('Loading data exercise 1 COMPLETE...')

print('Merging / Transforming data exercise 1...')
# Merging cons table with cons_email table on cons_id
merge_1 = pd.merge(
    left=cons[['cons_id', 'source', 'create_dt', 'modified_dt']],
    right=cons_email[cons_email['is_primary'] == 1][['cons_email_id', 'cons_id', 'email', 'is_primary']],
    on='cons_id',
    how='left'
)

# Merging merge_1 to subs table on cons_email_id
merge_2 = pd.merge(
    left=merge_1,
    right=subs[subs['chapter_id'] == 1][['cons_email_id', 'chapter_id', 'isunsub']],
    on='cons_email_id',
    how='left'
).astype({'isunsub': bool}) # Changes isunsub from int to bool

# Imports only columns needed 
merge_2 = merge_2[['email', 'source', 'isunsub', 'create_dt', 'modified_dt']]

# Changes dtypes from object to datetime
merge_2['create_dt'] = pd.to_datetime(merge_2['create_dt'], format='%a, %Y-%m-%d %H:%M:%S')
merge_2['modified_dt'] = pd.to_datetime(merge_2['modified_dt'], format='%a, %Y-%m-%d %H:%M:%S')

# Renames columns to naming convention per schema
merge_2.columns=['email', 'code', 'is_unsub', 'created_dt', 'updated_dt']

print('Merging / Transforming data exercise 1... COMPLETE')

# Converting to csv
merge_2.to_csv('people.csv', index=False)
print('people.csv SAVED')

# --------------------------------------------------------------

# EXERCISE 2

print('Loading data exercise 2...')
# Imported csv from exercise 1
people = pd.read_csv('people.csv', usecols=['email', 'created_dt'], parse_dates=['created_dt'])
print('Loading data exercise 2...COMPLETE')

print('Merging / Transforming data exercise 2...')
# Renames columns to naming convention per schema
people.columns=['email', 'acquisition_date']
#Change dtype from datetime to date
people['acquisition_date'] = people['acquisition_date'].dt.date

# A groupby to count the constituents acquired on specific date. Renamed the resulted aggregate column to acquisitions.
acquisitions = people.groupby('acquisition_date')['email'].count().rename('acquisitions')
print('Merging / Transforming data exercise 2... COMPLETE')

# Converting to csv
acquisitions.to_csv('acquisition_facts.csv')
print('acquisition_facts.csv SAVED')