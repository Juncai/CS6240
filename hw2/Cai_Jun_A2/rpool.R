for (i in 1:10) {
	c_carrier <- top_carriers[i]
	c_df <- res[carrier == c_carrier,]
	attach(c_df)
	c_sorted <- c_df[order(month)]
	plot(c_sorted$month, c_sorted$meanPrice)
}

