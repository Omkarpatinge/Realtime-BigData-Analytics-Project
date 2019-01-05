import matplotlib.pyplot as plt
monday=[4799,2347,1659,1352,1676,2299,4584,6732,13505,12695,11165,11088,11318,11664,13399,12596,15215,14538,12290,9515,7603,5968,5110,3912]
tuesday=[4261,1921,1357,1014,1149,2035,4550,6936,13901,13588,11813,11584,11790,12196,13911,12885,15688,15476,13297,10547,8229,6464,5559,4449]
wednesday=[4464,2120,1368,1138,1342,2033,4399,6844,13590,12714,11000,10753,11574,11758,14032,12934,15513,15519,13675,10654,8327,6810,5936,4566]
thursday=[4876,2244,1596,1319,1479,2106,4371,6652,13697,13039,11267,11239,11652,12115,13939,12967,15589,15488,13464,10758,8793,7311,6484,5060]
friday = [5584,2912,2102,1626,1934,2330,4565,6674,12960,12749,11048,11452,12056,12730,15252,14349,16629,16650,14105,11655,9605,7866,7323,6389]
saturday=[7181,4730,3969,3563,4111,3414,3178,3015,5689,6745,7902,9324,10254,11279,12351,10774,12751,11703,10810,9312,8566,7688,7523,6692]
sunday=[7229,5236,4310,4008,4698,3797,2839,2513,4147,4895,6236,7586,8728,9915,11518,9963,11634,10441,9504,8353,7420,6514,5929,4627]
x = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23]
plt.plot(x, monday,label="Monday")
plt.plot(x, tuesday,label="Tuesday")
plt.plot(x, wednesday,label="Wednesday")
plt.plot(x, thursday,label="Thursday")
plt.plot(x, friday,label="Friday")
plt.plot(x, saturday,label="Saturday")
plt.plot(x, sunday,label="Sunday")
plt.xlabel('Hour')
plt.ylabel('Number of accidents')
plt.title('Collision time period')
plt.legend()
plt.show()