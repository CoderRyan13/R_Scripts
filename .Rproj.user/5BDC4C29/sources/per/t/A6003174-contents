temp <- tempfile()

# Download zip archive to temp file
urlZipArchive = "https://github.com/CriticalPathTraining/PBI365/raw/master/Student/Data/SalesData.zip"
download.file(urlZipArchive, temp)

# Extract csv file out of zip
csvFile = unz(temp, "SalesByCustomer.csv")

# Load new data frame from data in csv file
customers <- read.csv(csvFile)

# Perform cleanup
unlink(temp)
remove(csvFile)
remove(temp)
remove(urlZipArchive)