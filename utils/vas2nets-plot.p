# output as png image
set terminal png

# save file to "out.png"
set output "vas2nets.png"

# graph title
set title "ab -n 100000 -c 50"

# nicer aspect ratio for image size
set size 1,0.7

# y-axis grid
set grid y

# x-axis label
set xlabel "request"

# y-axis label
set ylabel "response time (ms)"

plot "vas2nets.dat" using 9 smooth sbezier with lines title "vas2nets"