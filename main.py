import csv

rows = []
with open('ATBootcamp03 - PB - 01.csv', 'r') as f:
    reader = csv.reader(f)
    header = next(reader)

    ## for every row, replace \n\n with " "
    for row in reader:
        rows.append(row)
        for i in range(len(row)):
            row[i] = row[i].replace('\n\n', ' ')
            row[i] = row[i].replace('\n', ' ')
            row[i] = row[i].replace('\r', ' ')
            row[i] = row[i].replace('\t', ' ')
            row[i] = row[i].replace('\xa0', ' ')
            row[i] = row[i].replace('\u200b', ' ')
            row[i] = row[i].replace('\u200c', ' ')
            row[i] = row[i].replace('\u200d', ' ')
            row[i] = row[i].replace('\u200e', ' ')
            row[i] = row[i].replace('\u200f', ' ')
            row[i] = row[i].replace('\u202a', ' ')
            row[i] = row[i].replace('\u202b', ' ')
            row[i] = row[i].replace('\u202c', ' ')
            row[i] = row[i].replace('\u202d', ' ')
            row[i] = row[i].replace('\u202e', ' ')
            row[i] = row[i].replace('\u2060', ' ')
            row[i] = row[i].replace('\u2061', ' ')
            row[i] = row[i].replace('\u2062', ' ')
            row[i] = row[i].replace('\u2063', ' ')
            row[i] = row[i].replace('\u2064', ' ')
            row[i] = row[i].replace('\u2065', ' ')
            row[i] = row[i].replace('\u2066', ' ')
            row[i] = row[i].replace('\u2067', ' ')
            row
print(header)
print(rows)

## update the csv file
with open('ATBootcamp03 - PB - 01.csv', 'w') as f:
    writer = csv.writer(f)
    writer.writerow(header)
    writer.writerows(rows)

f.close()