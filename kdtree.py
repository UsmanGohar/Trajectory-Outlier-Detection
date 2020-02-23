import timeit
from math import sin, cos, sqrt, atan2, radians
from collections import defaultdict
from collections import Counter
import glob
import os
import multiprocessing as mp
#import gmplot
#import googlemaps, gmplot, webbrowser, os, json
#from scipy import spatial
import numpy as np
import sys
from decimal import *
import psutil
import time

sys.setrecursionlimit(10000)





#gmaps = googlemaps.Client(key='AIzaSyCul2f2Ao1xZ6_Lo3SdtxR-LuBeQjSysYw') 
#geocode_result = gmaps.geocode('Beijing')
#gmap = gmplot.GoogleMapPlotter(39.9042, 116.4074, 13 ) 
#gmaps = googlemaps.Client(key='AIzaSyCul2f2Ao1xZ6_Lo3SdtxR-LuBeQjSysYw')
#gmap.apikey = 'AIzaSyCul2f2Ao1xZ6_Lo3SdtxR-LuBeQjSysYw'



path = 'C:\\Users\\User\\Downloads\\T-drive Taxi Trajectories\\release\\taxi_log_2008_by_id\\*.txt'
#laptop path = '/home/usman/Downloads/release/taxi_log_2008_by_id/*.txt'
#path = '/home/csgrads/gohar011/Downloads/T-drive Taxi Trajectories/release/taxi_log_2008_by_id/*.txt' 

def distance(lat1, lon1, lat2, lon2):  #Function to determine distance between Geo-coordinates

    R = 6373.0

    lat1 = radians(lat1)
    lon1 = radians(lon1)
    lat2 = radians(lat2)
    lon2 = radians(lon2)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    #print("Result:", distance*1000)
    return distance*1000


def window(cur_day, cur_hour, cur_endtime):

    files = glob.glob(path)
    DBt = np.zeros((155355,2))       # Contains Trajectories & Points

    bins = list(range(33,48))        # To insert bins in order in array
    
    ID_list = []                     # Maintain list of the corresponding 
    
    i = 0
    temp = -1
    
    for file in files:
	
        offset = 0          # Use to offset from start of the trajectory bin in numpy array (Insert in correct position according to timebin)
        if i == 100000:         # CHANGE NUMBER OF FILES HERE!!!!
            break
        
        f = open(file,'r')
        ID = os.path.basename(f.name).split('.')[0]       #Trajectory ID (File name)
        ID_list.append(ID)
        for line in f:
            x = line.split(' ')
            
            date = x[0].split(',')  #List whose 2nd element contains date (yyyy-mm-dd) - date [1]
            time1 = x[1].split(',')  #List whose 1st element contains time (HH:MM::SS) - time[0]

            lat = time1[1]
            lon = time1[2]
            
            day = date[1].split('-')[2]      #Day
            hour = time1[0].split(':')[0]     #HOUR
            min_ = time1[0].split(':')[1]     #MINS
            secs = time1[0].split(':')[2]     #SECS

            
            if (cur_day == day and cur_hour == hour and int(cur_endtime) - int('15') < int(min_)  and min_ <= cur_endtime and min_!= temp):
                
                #print(ID, "\n")   # DEBUG 18/10
                #print(min_)
                offset = bins.index(int(min_))
                
                temp = min_
                DBt[(i*15) + offset][0] = lat              # Trajectories in window
                DBt[(i*15) + offset][1] = lon
		#if ID not in ID_list:
		#	ID_list.append(ID)

                #offset = offset + 1


        i = i+1

    #print (DBt)   # DEBUG 18/10
    #print (ID_list)  # DEBUG 18/10
                           
    return DBt, ID_list


def fill_gaps(DBt, cur_endtime, ID_list):

    #missing = list(map(str,range( int(cur_endtime) - 14, int(cur_endtime) + 1 )))
    Final_ID = []
    missing = []
    jump = 15
    new_DBt = DBt

    for i in range(0,len(DBt)//15):
        
        test = DBt[15*i:(15*i) + jump] # Slice of 15 min window from array

        all_zeros = not test.any()    # Check for empty windows (from files)

        if all_zeros or i == 1493:	# Problem with 245 file

            continue

        missing = np.where(test>0)     # Find index of non zero entries


        lat = Decimal(DBt[(15*i) + missing[0][0]][0])
        long = Decimal(DBt[(15*i) + missing[0][0]][1])

	#print ("the array",missing[0])
	
	#print (i)
	#print (test)
	#print (DBt[(15*i) + missing[0][len(missing) - 1] ][1])

	
        diff_lat = (Decimal(DBt[(15*i) + missing[0][0]][0]) - Decimal(DBt[(15*i) + missing[0][len(missing) - 1] ][0])) /( 15)
        diff_long = (Decimal(DBt[(15*i) + missing[0][0]][1]) - Decimal(DBt[(15*i) + missing[0][len(missing) - 1]][1])) / ( 15)
	
	#print (diff_lat)

        for j in range(0,15):

            if DBt[(15*i) + j][0] == 0:

                lat = lat + diff_lat
                long = long + diff_long

                DBt[(15*i) + j][0] = lat      # fill the gaps
                DBt[(15*i) + j][1] = long     # fill the gaps
     
        Final_ID.append(ID_list[i])

        #for j in range(0,len(test)):

            
        #print (all_zeros, '\n')

        #print (i,test, jump,'\n')

    #print (DBt)

        #if DBt[i][0] == 0 & DBt[i][1] == 0:

           
    return DBt
    
def neighbor_timebins(DBt):   # This generates the graph and contains all the neighbors

    TR_list = defaultdict(lambda: defaultdict(list))
    i=2

    for key in DBt:
        i=2
        for l in range(0,len(DBt[key])):
            if (i <= len(DBt[key])):

                for s_key in DBt:

                    if (s_key == key):
                        continue
                        print ('next')
            
                    k = 2

                    #print (key,s_key)

                    for j in range(0,len(DBt[s_key])):
                        if k <= len(DBt[s_key]):

                            lat1 = float(DBt[key][i-2])         # Latitude 1
                            lon1 = float(DBt[key][i-1])         # Longitude 1
                            lat2 = float(DBt[s_key][k-2])       # Latitude 2
                            lon2 = float(DBt[s_key][k-1])       # Longitude 2

                            timebin = DBt[s_key][k]

                            if DBt[key][i] == DBt[s_key][k]:

                                d = distance(lat1,lon1,lat2,lon2)
                            
                                if d <= 500:
                                    #print (key,s_key)
                                    #print (lat1,lon1,lat2,lon2)
                                    TR_list[key][timebin].append(s_key)

                            k = k + 3        # The format is such that every 4th element is a new trajectory point
                    
            i = i + 3

    for key in TR_list:
        print (key, ':', TR_list[key])
        print ('\n')
    return (TR_list)


def trajectory_outlier(k, threshold, TR_list):   # Actual algorithm checking for k number of trajectories and threshold
    outliers = []
    inliers = []

    for key in TR_list:

        if len(TR_list[key]) >= threshold:
            print("check")
            for t_bins in TR_list[key]:

                for i in TR_list[key][t_bins]:
                    a=list(TR_list[key].values()) 
                    total=[]
                    for j in a:
                        total = total + j
                    final = Counter(total)        # Contains the count of the number of occurences of each point of an ID for each Trajectory

                    print(key,final)          
                if len(final) >= k:
                    num = 0
                    #print (key, final)             # DEBUG
                    for items in final:
                        
                        if final[items] >= threshold:
                            num = num +1             # Check whether number of neighbor trajectories >= k
                    if num >= k:
                        #print (key, "In_line")      # DEBUG
                        if key not in inliers:
                            inliers.append(key)
                        #print (num)                  # DEBUG
                        dummy=0
                    else:
                        #print("outlier")             # DEBUG
                        if key not in outliers:
                            outliers.append(key)
                            
                    #print (len(final))              # DEBUG
                else:
                    if key not in outliers:
                        outliers.append(key)
                    
        else:
            if key not in outliers:
                outliers.append(key)
            #print ("outliers")                     # DEBUG

#   print (num)
    return outliers, inliers
            
def tree_dist(traj_arr):

    final = []
    jump = 15
    for i in range(0,(len(traj_arr))/15):

        index = range(15*i,jump)

        new_a = np.delete(traj_arr, index,0)
        tree = spatial.KDTree(new_a)

        for j in range(15*i, jump):
		#print j
                neighbors = tree.query_ball_point([traj_arr[j][0],traj_arr[j][1]], 0.002)
	        #print (neighbors, '\n')
                final.append(neighbors)
        jump = jump + 15

    	#print (final)

def tree_once(tree,traj_arr,affinity):

    final = []
    print (affinity)
    proc = psutil.Process()
    print('PID: {pid}'.format(pid=proc.pid))
    aff = proc.cpu_affinity()
    print('Affinity before: {aff}'.format(aff=aff))
    proc.cpu_affinity([affinity])
    aff = proc.cpu_affinity()
    print('Affinity after: {aff}'.format(aff=aff))

    for i in range(0,len(traj_arr)):
    	neighbors = tree.query_ball_point([traj_arr[i][0],traj_arr[i][1]], 0.00000000001)
    	final.append(neighbors)
        #print (traj_arr[i], neighbors,os.getpid())
    print(len(final))

    print(psutil.cpu_percent())


#print(TR_list[1][])  
#print (TR_list)

def spawn(tree,data_cores):
     
    procs = list()
    n_cpus = 4
    j = 0
    for cpu in range(n_cpus):
        affinity = [cpu]
        d123 = dict(affinity=affinity)
        for i in range(j,n_cpus):
        	p = mp.Process(target = tree_once, args = (tree,data_cores[i],cpu+1))
        	j = i + 1
        	break
        p.start()
        procs.append(p)
        
    start = time.time()

    for p in procs:
        p.join()
        print('joined')
    end = time.time()
    print('Time', end - start)

if __name__=='__main__':

    # Checking number of CPU cores
    ans, ids = window('02','15','47')

    ans = np.array(ans)
    ans = fill_gaps(ans, '47', ids)
    ans = ans[~np.all(ans == 0, axis=1)]
    tree = spatial.KDTree(ans)

    print ("Cores",mp.cpu_count())
    processes = []
    data_cores = []
    sc = 4
       
    for i in range(0,sc):

        a = ans[int((i*len(ans)/sc)+1) : int((i+1)*len(ans)/sc),:]
        data_cores.append(a)

        spawn(tree, data_cores)
        #processes = [mp.Process(target = tree_once, args = (tree,data_cores[k],k)) for k in range(0,len(data_cores))]
        

        #for p in processes:
        #    p.start()

        #for p in processes:
        #    p.join()


#   a = ans[0:len(ans)/4,:]                             # 4 cores
#   b = ans[(len(ans)/4)+1:len(ans)/2,:]
#   c = ans[(len(ans)/2)+1:3*len(ans)/4,:]
#   d = ans[(3*len(ans)/4)+1:len(ans)-1,:]


#    a = ans[0:len(ans)/8,:]                             # 8 cores
#    b = ans[(len(ans)/8)+1:2*len(ans)/8,:]
#    c = ans[(2*len(ans)/8)+1:3*len(ans)/8,:]
#    d = ans[(3*len(ans)/8)+1:4*len(ans)/8,:]
#    e = ans[(4*len(ans)/8)+1:5*len(ans)/8,:]
#    f = ans[(5*len(ans)/8)+1:6*len(ans)/8,:]
#    g = ans[(6*len(ans)/8)+1:7*len(ans)/8,:]
#    h = ans[(7*len(ans)/8)+1:len(ans)-1,:]

#    a = ans[0:len(ans)/32,:]                             # 16 cores
#    b = ans[(len(ans)/32)+1:2*len(ans)/32,:]
#    c = ans[(2*len(ans)/32)+1:3*len(ans)/32,:]
#    d = ans[(3*len(ans)/32)+1:4*len(ans)/32,:]
#    e = ans[(4*len(ans)/32)+1:5*len(ans)/32,:]
#    f = ans[(5*len(ans)/32)+1:6*len(ans)/32,:]
#    g = ans[(6*len(ans)/32)+1:7*len(ans)/32,:]
#    h = ans[(7*len(ans)/32)+1:8*len(ans)/32,:]
#    a1 = ans[(8*len(ans)/32)+1:9*len(ans)/32,:]                            
#    b2 = ans[(9*len(ans)/32)+1:10*len(ans)/32,:]
#    c3 = ans[(10*len(ans)/32)+1:11*len(ans)/32,:]
#    d4 = ans[(11*len(ans)/32)+1:12*len(ans)/32,:]
#    e5 = ans[(12*len(ans)/32)+1:13*len(ans)/32,:]
#    f6 = ans[(13*len(ans)/32)+1:14*len(ans)/32,:]
#    g7 = ans[(14*len(ans)/32)+1:15*len(ans)/32,:]
#    h8 = ans[(15*len(ans)/32)+1:16*len(ans)/32,:]
#    a11 = ans[(16*len(ans)/32)+1:17*len(ans)/32,:]                            
#    b22 = ans[(17*len(ans)/32)+1:18*len(ans)/32,:]
#    c33 = ans[(18*len(ans)/32)+1:19*len(ans)/32,:]
#    d44 = ans[(19*len(ans)/32)+1:20*len(ans)/32,:]
#    e55 = ans[(20*len(ans)/32)+1:21*len(ans)/32,:]
#    f66 = ans[(21*len(ans)/32)+1:22*len(ans)/32,:]
#    g77 = ans[(22*len(ans)/32)+1:23*len(ans)/32,:]
#    h88 = ans[(23*len(ans)/32)+1:24*len(ans)/32,:]
#    a111 = ans[(24*len(ans)/32)+1:25*len(ans)/32,:]                            
#    b222 = ans[(25*len(ans)/32)+1:26*len(ans)/32,:]
#    c333 = ans[(26*len(ans)/32)+1:27*len(ans)/32,:]
#    d444 = ans[(27*len(ans)/32)+1:28*len(ans)/32,:]
#    e555 = ans[(28*len(ans)/32)+1:29*len(ans)/32,:]
#    f666 = ans[(29*len(ans)/32)+1:30*len(ans)/32,:]
#    g777 = ans[(30*len(ans)/32)+1:31*len(ans)/32,:]
#    h888 = ans[(31*len(ans)/32)+1:len(ans)-1,:]
    

    



    #a = ans[0:len(ans)/2,:]
    #b = ans[(len(ans)/2)+1:len(ans),:]

#    p = mp.Process(target = tree_once, args = (tree,a,))
#    p1 = mp.Process(target = tree_once, args = (tree,b,))
#    p2 = mp.Process(target = tree_once, args = (tree,c,))
#    p3 = mp.Process(target = tree_once, args = (tree,d,))
#    p4 = mp.Process(target = tree_once, args = (tree,e,))
#    p5 = mp.Process(target = tree_once, args = (tree,f,))
#    p6 = mp.Process(target = tree_once, args = (tree,g,))
#    p7 = mp.Process(target = tree_once, args = (tree,h,))
#    p8 = mp.Process(target = tree_once, args = (tree,a1,))
#    p9 = mp.Process(target = tree_once, args = (tree,b2,))
#    p10 = mp.Process(target = tree_once, args = (tree,c3,))
#    p11 = mp.Process(target = tree_once, args = (tree,d4,))
#    p12 = mp.Process(target = tree_once, args = (tree,e5,))
#    p13= mp.Process(target = tree_once, args = (tree,f6,))
#    p14 = mp.Process(target = tree_once, args = (tree,g7,))
#    p15 = mp.Process(target = tree_once, args = (tree,h8,))
#    p16 = mp.Process(target = tree_once, args = (tree,a11,))
#    p17 = mp.Process(target = tree_once, args = (tree,b22,))
#    p18 = mp.Process(target = tree_once, args = (tree,c33,))
#    p19 = mp.Process(target = tree_once, args = (tree,d44,))
#    p20 = mp.Process(target = tree_once, args = (tree,e55,))
#    p21 = mp.Process(target = tree_once, args = (tree,f66,))
#    p22 = mp.Process(target = tree_once, args = (tree,g77,))
#    p23 = mp.Process(target = tree_once, args = (tree,h88,))
#    p24 = mp.Process(target = tree_once, args = (tree,a111,))
#    p25 = mp.Process(target = tree_once, args = (tree,b222,))
#    p26 = mp.Process(target = tree_once, args = (tree,c333,))
#    p27 = mp.Process(target = tree_once, args = (tree,d444,))
#    p28 = mp.Process(target = tree_once, args = (tree,e555,))
#    p29 = mp.Process(target = tree_once, args = (tree,f666,))
#    p30 = mp.Process(target = tree_once, args = (tree,g777,))
#    p31 = mp.Process(target = tree_once, args = (tree,h888,))


#    p.start()
#    p1.start()
#    p2.start()
#    p3.start()
#    p4.start()
#    p5.start()
#    p6.start()
#    p7.start()
#    p8.start()
#    p9.start()
#    p10.start()
#    p11.start()
#    p12.start()
#    p13.start()
#    p14.start()
#    p15.start()
#    p16.start()
#    p17.start()
#    p18.start()
#    p19.start()
#    p20.start()
#    p21.start()
#    p22.start()
#    p23.start()
#    p24.start()
#    p25.start()
#    p26.start()
#    p27.start()
#    p28.start()
#    p29.start()
#    p30.start()
#    p31.start()

#    p.join()
#    p1.join()
#    p2.join()
#    p3.join()
#    p4.join()
#    p5.join()
#    p6.join()
#    p7.join()
#    p8.join()
#    p9.join()
#    p10.join()
#    p11.join()
#    p12.join()
#    p13.join()
#    p14.join()
#    p15.join()
#    p16.join()
#    p17.join()
#    p18.join()
#    p19.join()
#    p20.join()
#    p21.join()
#    p22.join()
#    p23.join()
#    p24.join()
#    p25.join()
#    p26.join()
#    p27.join()
#    p28.join()
#    p29.join()
#    p30.join()
#    p31.join()


#tree_once(ans)

#print ((ans['1'][2]))

#check = neighbor_timebins(ans)

#out, inl = trajectory_outlier(2, 3, check)

#print("inlier", len(inl), "outlier",len(out))        #LENGTH

''' PLOTTING ALL COORDINATES PORTION
#start 
latitude_list = []
longitude_list = []
j=0

for ID in sorted(out):
    latitude_list = []
    longitude_list = []
    for i in range(0,len(ans[ID])):
        j = j + 1
        if j == 3:
            j = 0
            continue
        if j == 1:
            ans[ID][i] = ans[ID][i].rstrip()
            latitude_list.append(float(ans[ID][i]))
        if j == 2:
            ans[ID][i].rstrip()
            longitude_list.append(float(ans[ID][i]))
    
    #print(ID,'\n')
    #print(ans[ID],'\n')
    #print(latitude_list, '\n')
    #print(longitude_list, '\n')
    longitude_list.sort()
    latitude_list.sort()
    #print(latitude_list)
    gmap.scatter(longitude_list, latitude_list, '# 0000FF', 
                              size = 30, marker = False )
    gmap.plot(longitude_list, latitude_list, 
           'red', edge_width = 6)

latitude_list1 = []
longitude_list1 = []   
for ID in sorted(inl):
    latitude_list1 = []
    longitude_list1 = []
    for i in range(0,len(ans[ID])):
        j = j + 1
        if j == 3:
            j = 0
            continue
        if j == 1:
            ans[ID][i] = ans[ID][i].rstrip()
            latitude_list1.append(float(ans[ID][i]))
        if j == 2:
            ans[ID][i].rstrip()
            longitude_list1.append(float(ans[ID][i]))
    
    #print(ID,'\n')
    #print(ans[ID],'\n')
    #print(latitude_list1, '\n')
    #print(longitude_list1, '\n')
    longitude_list1.sort()
    latitude_list1.sort()
    gmap.scatter(longitude_list1, latitude_list1, '# FF0000', 
                              size = 40, marker = False )
    gmap.plot(longitude_list1, latitude_list1, 
           'cornflowerblue', edge_width = 5)


    

#print (len(latitude_list))
#print (len(longitude_list))

#print (latitude_list)
#print("Outlier", out, "Inlier", inl)

# end


'''
# Draw

##### TO OPEN IN WEB BROWSER

# gmap.draw("my_map.html")

# filename = 'file:///'+os.getcwd()+'/' + 'my_map.html'
# webbrowser.open_new_tab(filename)



