#import xlwt

#RUN PIP INSTALL XLSXWRITER
import xlsxwriter

def to_spreadsheet(guessed_encoding, column_names_list, guessed_datatype_list, spreadsheet_name):

  #book = xlwt.Workbook(encoding=guessed_encoding)
  workbook = xlsxwriter.Workbook(spreadsheet_name+".csv")

  #sheet1 = book.add_sheet("Input Redshift Datatype Spreadsheet")
  worksheet = workbook.add_worksheet()

  worksheet.write(0, 0, "Column Name")
  worksheet.write(0, 1, "Guessed Datatype")
  worksheet.write(0, 2, "User Input Datatype")

  def _list_to_excel_column(list_of_entries, column_to_write_to):
    """
    list_of_entries: list of entries to want to write into individual cells for a column in the excel sheet
      possible arguments are column_names_list and guessed_datatype_list
    """
    i = 1
    for n in list_of_entries:
      worksheet.write(i, column_to_write_to, n)
      i = i + 1

  _list_to_excel_column(column_names_list, 0)
  _list_to_excel_column(guessed_datatype_list, 1)

  #book.save(spreadsheet_name+".xls")



