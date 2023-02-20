package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v3"
)

type Column struct {
	ColumnName string
	ColumnType string
}

func main() {
	var tableName string
	var credsFilePath string

	app := &cli.App{
		Name:        "Make History Table tool",
		Description: "Easily generate history tables from source tables",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "table_name",
				Usage:       "Name of table to generate history table for",
				Destination: &tableName,
			},
			&cli.StringFlag{
				Name:        "creds_file",
				Usage:       "Path to credentials YAML",
				Destination: &credsFilePath,
			},
		},
		Action: func(ctx *cli.Context) error {
			credsFile, err := ioutil.ReadFile(credsFilePath)
			if err != nil {
				log.Fatal(err)
			}

			credsData := make(map[interface{}]interface{})
			err = yaml.Unmarshal([]byte(credsFile), &credsData)
			if err != nil {
				log.Fatal(err)
			}

			connStr := fmt.Sprintf("%v:%v@tcp(%v:%v)/%v", credsData["user"], credsData["password"], credsData["host"], credsData["port"], credsData["db"])

			db, err := sql.Open("mysql", connStr)
			if err != nil {
				log.Fatal(err)
			}

			err = db.Ping()
			if err != nil {
				log.Fatal(err)
			}

			rows, err := db.Query(fmt.Sprintf("DESC %s", tableName))
			if err != nil {
				log.Fatal(err)
			}

			var (
				columnName    string
				columnType    string
				columnNull    string
				columnKey     string
				columnDefault *string
				columnExtra   string
			)

			var columnsAll []Column

			defer rows.Close()
			for rows.Next() {
				err := rows.Scan(&columnName, &columnType, &columnNull, &columnKey, &columnDefault, &columnExtra)
				if err != nil {
					log.Fatal(err)
				}

				columnsAll = append(columnsAll, Column{
					ColumnName: columnName,
					ColumnType: columnType,
				})
			}

			var primaryKeyName string

			fmt.Print("Enter primary key column name: ")
			fmt.Scanln(&primaryKeyName)

			primaryKeyName = strings.TrimSpace(primaryKeyName)
			if len(primaryKeyName) == 0 {
				log.Fatal(errors.New("primary key not provided"))
			}

			var primaryKeyType string

			for _, column := range columnsAll {
				if column.ColumnName == primaryKeyName {
					primaryKeyType = column.ColumnType
					break
				}
			}

			if len(primaryKeyType) == 0 {
				log.Fatal(errors.New("primary key is not in columns list"))
			}

			var columnsInHistoryTable []Column
			for _, column := range columnsAll {
				if column.ColumnName == primaryKeyName {
					continue
				}

				var userInput string

				for {
					fmt.Printf("Include '%s' column in history table (y/n)? ", column.ColumnName)
					fmt.Scanln(&userInput)

					if userInput == "y" {
						columnsInHistoryTable = append(columnsInHistoryTable, column)
						break
					} else if userInput == "n" {
						break
					}
				}
			}

			// create output directory if it doesn't exist
			outputDir := "output"
			if _, err := os.Stat(outputDir); errors.Is(err, os.ErrNotExist) {
				err := os.Mkdir(outputDir, os.ModePerm)
				if err != nil {
					log.Fatal(err)
				}
			}

			file1, err := os.Create("output/create_table.sql")
			if err != nil {
				log.Fatal(err)
			}
			defer file1.Close()

			fmt.Fprint(file1, getCreateTableSQL(tableName, primaryKeyName, primaryKeyType, columnsInHistoryTable))

			file2, err := os.Create("output/after_insert_trigger.sql")
			if err != nil {
				log.Fatal(err)
			}
			defer file2.Close()

			fmt.Fprint(file2, getAfterInsertTriggerSQL(tableName, primaryKeyName, columnsInHistoryTable))

			file3, err := os.Create("output/after_update_trigger.sql")
			if err != nil {
				log.Fatal(err)
			}
			defer file3.Close()

			fmt.Fprint(file3, getAfterUpdateTriggerSQL(tableName, primaryKeyName, columnsInHistoryTable))

			fmt.Println("Output files written successfully")

			return nil
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func getHistoryTableName(tableName string) string {
	return fmt.Sprintf("%s_history", tableName)
}

func getNewColumnName(columnName string) string {
	return fmt.Sprintf("new_%s", columnName)
}

func getOldColumnName(columnName string) string {
	return fmt.Sprintf("old_%s", columnName)
}

func getPKColumnName(tableName string, columnName string) string {
	return fmt.Sprintf("%s_%s", tableName, columnName)
}

func getCreateTableSQL(
	tableName string,
	primaryKeyName string,
	primaryKeyType string,
	columns []Column,
) string {

	var columnLines []string
	columnLines = append(columnLines, "id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY")

	columnLines = append(columnLines, fmt.Sprintf("%s %s",
		getPKColumnName(tableName, primaryKeyName), primaryKeyType))

	for _, column := range columns {
		columnLines = append(columnLines, fmt.Sprintf("%s %s", getOldColumnName(column.ColumnName), column.ColumnType))
		columnLines = append(columnLines, fmt.Sprintf("%s %s", getNewColumnName(column.ColumnName), column.ColumnType))
	}

	joinedColumnLines := strings.Join(columnLines, ",\n\t")

	result := fmt.Sprintf(`
	CREATE TABLE %s (%s);`,
		getHistoryTableName(tableName), joinedColumnLines)

	return result

}

func getAfterInsertTriggerSQL(
	tableName string,
	primaryKeyName string,
	columns []Column,
) string {
	triggerName := fmt.Sprintf("trg_%s_after_insert", tableName)

	var columnNames []string
	var insertValues []string
	for _, column := range columns {
		if column.ColumnName == primaryKeyName {
			columnNames = append(columnNames, getPKColumnName(tableName, primaryKeyName))
		} else {
			columnNames = append(columnNames, getNewColumnName(column.ColumnName))
		}

		insertValues = append(insertValues, fmt.Sprintf("NEW.%s", column.ColumnName))
	}

	joinedColumnNames := strings.Join(columnNames, ",\n\t\t\t\t")
	joinedInsertValues := strings.Join(insertValues, ",\n\t\t\t\t")

	result := fmt.Sprintf(`
	DELIMITER $$ 

	CREATE TRIGGER %s
	AFTER INSERT ON %s 
	FOR EACH ROW
		BEGIN
		INSERT INTO
			%s (%s)
		VALUES
			(%s);
		END$$

	DELIMITER ;
	`, triggerName, tableName,
		getHistoryTableName(tableName), joinedColumnNames, joinedInsertValues)

	return result
}

func getAfterUpdateTriggerSQL(
	tableName string,
	primaryKeyName string,
	columns []Column,
) string {
	triggerName := fmt.Sprintf("trg_%s_after_update", tableName)

	var columnNames []string
	var insertValues []string
	var ifConditions []string
	for _, column := range columns {
		if column.ColumnName == primaryKeyName {
			columnNames = append(columnNames, getPKColumnName(tableName, primaryKeyName))
			insertValues = append(insertValues, fmt.Sprintf("OLD.%s", column.ColumnName))
		} else {
			columnNames = append(columnNames, getOldColumnName(column.ColumnName))
			insertValues = append(insertValues, fmt.Sprintf("OLD.%s", column.ColumnName))

			columnNames = append(columnNames, getNewColumnName(column.ColumnName))
			insertValues = append(insertValues, fmt.Sprintf("NEW.%s", column.ColumnName))

			ifConditions = append(ifConditions, fmt.Sprintf("OLD.%s != NEW.%s", column.ColumnName, column.ColumnName))
		}
	}

	joinedIfConditions := strings.Join(ifConditions, " OR\n\t\t\t")
	joinedColumnNames := strings.Join(columnNames, ",\n\t\t\t\t")
	joinedInsertValues := strings.Join(insertValues, ",\n\t\t\t\t")

	result := fmt.Sprintf(`
	DELIMITER $$ 

	CREATE TRIGGER %s
	AFTER UPDATE ON %s 
	FOR EACH ROW
		BEGIN
		IF
			%s
		THEN
			INSERT INTO
				%s (%s)
			VALUES
				(%s); 
		END IF;
		END$$


	DELIMITER ;
	`, triggerName, tableName, joinedIfConditions,
		getHistoryTableName(tableName), joinedColumnNames, joinedInsertValues)

	return result
}
