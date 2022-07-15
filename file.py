import csv

rows = []
with open('ATBootcamp03 - PB - 01.csv', 'r') as f:
    reader = csv.reader(f)
    header = next(reader)
    for row in reader:
        rows.append(row)
        for i in range(len(row)):
            row[i] = row[i].replace('\n', ' ')

print(header)
print(rows)

with open('ATBootcamp03 - PB - 01.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(header)
    writer.writerows(rows)

f.close()