

run:
	rm -rf out
	sbt run

clean:
	rm -rf out derby.log metastore_db project target


report:
	Rscript RTest.R <path_of_output.csv>/output.csv <output_path>
	Rscript -e "rmarkdown::render('Assignment06_Report.Rmd')" <path_of_output.csv>/output.csv
