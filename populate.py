#!/usr/bin/python

import random

afile = open("random.txt", "w")

try:
	for i in range(int(input('How many random numbers?: '))):
		line = str(random.randint(-10000, 10000))
		afile.write(line + '\n')
		print(line)
except ValueError:
	print('Error')

afile.close()
