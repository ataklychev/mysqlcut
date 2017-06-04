package main

import (
	"os"
	"fmt"
	"bufio"
	"regexp"
	"flag"
	"strings"
	"errors"
)

type exclude []string
type excludeMap map[string]bool

/**
 * Исключает из входящего потока данные для указанных таблиц в mysql дампе
 * Данные принимает через stdin и тутже выдает результат в stdout
 * Используется когда нужно вырезать из дампа не нужные данные, особенно когда их слишком много.
 *
 * Пример использования:
 * unxz -c somedb.sql.xz | mysqlcut -e="logs,logs_extra" | mysql -u root -p somedb
 */
func main() {

	// парсим список таблиц, данные для которых нужно исключить из SQL дампа
	err, excludeMap := parseExcludeMap()

	// проверяем, что пользователь указал обязательный параметр со списком таблиц
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// проверяем, что содержимое SQL дампа подается через stdin
	if !hasPipe() {
		fmt.Println("no pipe :(")
		os.Exit(1)
	}

	// регулярка, начало описания структуры таблицы
	reStructure := regexp.MustCompile("Table structure for table `([^`]+)`")

	// регулярка, начало описания данных таблицы
	reData := regexp.MustCompile("Dumping data for table `([^`]+)`")

	// создание объектов для работы с stdin, stdout
	scanner := bufio.NewScanner(os.Stdin)
	writer := bufio.NewWriter(os.Stdout);
	defer writer.Flush()
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	match := make([]string, 2);

	// если true то в stdout ничего не выводится
	muted := false

	// построчно читаем поток из stdin
	for scanner.Scan() {
		// пропускаем строки длиннее 100 символов
		if (len(scanner.Text()) < 100) {

			// проверяем, если начинается структура новой таблицы
			match = reStructure.FindStringSubmatch(scanner.Text())
			if len(match) == 2 {
				muted = false
			}

			// проверяем, если начинаются данные новой таблицы
			match = reData.FindStringSubmatch(scanner.Text())
			if len(match) == 2 {
				if (excludeMap[match[1]]) {
					// если данные для исключаемой таблицы, включаем muted
					muted = true
				} else {
					// если данные для новой таблицы, выключаем muted
					muted = false
				}
			}
		}

		// если нужно, выводим данные в stdout
		if (!muted) {
			writer.Write(scanner.Bytes())
			writer.Write([]byte("\n"))
		}
	}
}

func hasPipe() bool {
	fi, err := os.Stdin.Stat();
	if err != nil {
		panic(err)
	}
	return fi.Mode()&os.ModeNamedPipe != 0;
}

/**
 * Парсинг списка таблиц из аргументов
 */
func parseExcludeMap() (error, excludeMap) {

	var res excludeMap = make(excludeMap)

	// описание флагов командной строки
	exclude := flag.String("e", "", "таблицы через запятую")

	// парсинг флагов командной строки
	flag.Parse()

	// проверяем что указаны таблицы для исключения
	if (len(*exclude) == 0) {
		return errors.New("Не указан списо таблиц исключений"), nil
	}

	// заполняем exclude table MAP
	excludeSlice := strings.Split(*exclude, ",")
	for _, excludeTable := range excludeSlice {
		res[excludeTable] = true
	}

	return nil, res
}
