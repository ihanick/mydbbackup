package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
)

var restore = flag.String("restore", "", "Restore backup from file")
var destination = flag.String("destination", "", "Restore destination directory")

func DbBackupSimple() (int, error) {
	var lsn int
	lsn = 0
	completion_flag := false
	cmd := exec.Command("ssh", "root@dev", "innobackupex", "--stream=xbstream", "--compress", "--compress-threads=4", "--parallel=4", "/root")
	stderr, err := cmd.StderrPipe()

	if err != nil {
		log.Fatal(err)
		return 0, fmt.Errorf("Backup finished with error: %v", err)
	}

	stdout, err1 := cmd.StdoutPipe()
	if err1 != nil {
		log.Fatal(err1)
		return 0, fmt.Errorf("Backup finished with error: %v", err1)
	}

	outputFile, err2 := os.Create("backup.xbstream")
	if err2 != nil {
		log.Fatal(err2)
		return 0, fmt.Errorf("Backup finished with error: %v", err2)
	}

	defer outputFile.Close()

	if err := cmd.Start(); err != nil {
		log.Fatal(err)
		return 0, fmt.Errorf("Backup finished with error: %v", err)
	}

	go io.Copy(outputFile, stdout)

	scanner := bufio.NewScanner(stderr)
	for scanner.Scan() {
		text := scanner.Text()
		fmt.Println("out:", text)
		if strings.Contains(text, "innobackupex: completed OK!") {
			completion_flag = true
		} else if strings.Contains(text, "The latest check point (for incremental)") {
			if _, err := fmt.Sscanf(text, "xtrabackup: The latest check point (for incremental): '%d'\n", &lsn); err != nil {
				log.Fatal(err)
				return 0, fmt.Errorf("Backup finished with error: %v", err)
			}

			fmt.Println("LSN:", lsn)
		}

	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
		return 0, fmt.Errorf("Backup finished with error: %v", err)
	}

	if completion_flag == true {
		os.Rename("backup.xbstream", fmt.Sprintf("backup-%d.xbstream", lsn))
		return lsn, nil
	} else {
		return 0, fmt.Errorf("Backup finished with error %s", "Unknown Error")
	}
}

func DbRestoreSimple(restore_from string, restore_to string) (string, error) {
	// ssh root@dev mkdir /root/restore
	{
		cmd := exec.Command("ssh", "root@dev", "mkdir", restore_to)
		stderr, err := cmd.StderrPipe()

		if err != nil {
			log.Fatal(err)
			return "", fmt.Errorf("Restore finished with error: %v", err)
		}
		if err := cmd.Start(); err != nil {
			log.Fatal(err)
			return "", fmt.Errorf("Backup finished with error: %v", err)
		}
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			text := scanner.Text()
			log.Fatal(text)
			return "", fmt.Errorf("Restore finished with error: %v", text)
		}
	}

	// cat backup-11618166.xbstream | ssh root@dev xbstream -C /root/restore -x
	{
		cmd := exec.Command("ssh", "root@dev", "xbstream", "-C", restore_to, "-x")
		runCmdFromStdinWorks(cmd, populateStdin(restore_from))
	}
	// ssh root@dev innobackupex --decompress /root/restore
	{
		completion_flag := false
		cmd := exec.Command("ssh", "root@dev", "innobackupex", "--decompress", restore_to)
		stderr, err := cmd.StderrPipe()

		if err != nil {
			log.Fatal(err)
			return "", fmt.Errorf("decompress finished with error: %v", err)
		}

		if err := cmd.Start(); err != nil {
			log.Fatal(err)
			return "", fmt.Errorf("decompress finished with error: %v", err)
		}

		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			text := scanner.Text()
			fmt.Println("decompress:", text)
			if strings.Contains(text, "innobackupex: completed OK!") {
				completion_flag = true
			}
		}

		if err := scanner.Err(); err != nil {
			log.Fatal(err)
			return "", fmt.Errorf("decompress finished with error: %v", err)
		}

		if completion_flag == false {
			return "", fmt.Errorf("decompress finished with error %s", "Unknown Error")
		}

	}
	// "innobackupex: completed OK!"
	// ssh root@dev innobackupex --apply-log /root/restore
	{
		completion_flag := false
		cmd := exec.Command("ssh", "root@dev", "innobackupex", "--apply-log", restore_to)
		stderr, err := cmd.StderrPipe()

		if err != nil {
			log.Fatal(err)
			return "", fmt.Errorf("apply-log finished with error: %v", err)
		}

		if err := cmd.Start(); err != nil {
			log.Fatal(err)
			return "", fmt.Errorf("apply-log finished with error: %v", err)
		}

		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			text := scanner.Text()
			fmt.Println("apply-log out:", text)
			if strings.Contains(text, "innobackupex: completed OK!") {
				completion_flag = true
			}
		}

		if err := scanner.Err(); err != nil {
			log.Fatal(err)
			return "", fmt.Errorf("apply-log finished with error: %v", err)
		}

		if completion_flag == false {
			return "", fmt.Errorf("apply-log finished with error %s", "Unknown Error")
		}


	}
	// ssh root@dev cat /root/restore/xtrabackup_binlog_info
	change_master_info := ""
	{
		cmd := exec.Command("ssh", "root@dev", "cat", "/root/restore/xtrabackup_binlog_info", restore_to)
		stdout, err := cmd.StdoutPipe()

		if err != nil {
			log.Print(err)
			return "", nil
		}

		if err := cmd.Start(); err != nil {
			log.Print(err)
			return "", nil
		}

		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			text := scanner.Text()
			fmt.Println("binlog pos out:", text)
			binloginfo :=  strings.Split(text, "\t")
			change_master_info = fmt.Sprintf("CHANGE MASTER TO MASTER_LOG_FILE='%s', MASTER_LOG_POS=%s;\n", binloginfo[0], binloginfo[1]);
		}

		if err := scanner.Err(); err != nil {
			log.Print(err)
			return "", nil
		}
	}	// mysql-bin.000001	500



	return change_master_info, nil
}

func populateStdin(str string) func(io.WriteCloser) {
	return func(stdin io.WriteCloser) {
		defer stdin.Close()

		// open input file
		fi, err := os.Open(str)
		if err != nil { panic(err) }
		// close fi on exit and check for its returned error
		defer func() {
			if err := fi.Close(); err != nil {
				panic(err)
			}
		}()

		// make a read buffer
		r := bufio.NewReader(fi)

		io.Copy(stdin, r)
	}
}

func runCmdFromStdinWorks(cmd *exec.Cmd, populate_stdin_func func(io.WriteCloser)) {
	stdin, err := cmd.StdinPipe()
	if err != nil {
		log.Panic(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Panic(err)
	}
	err = cmd.Start()
	if err != nil {
		log.Panic(err)
	}
	populate_stdin_func(stdin)
	io.Copy(os.Stdout, stdout)
	err = cmd.Wait()
	if err != nil {
		log.Panic(err)
	}
}

func main() {
	flag.Parse()
	if *restore == "" {
		lsn, err := DbBackupSimple()
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("Backup was finished,", "lsn:", lsn)
	} else {
		if *destination == "" {
			*destination = "/root/restore"
		}

		change_master, err := DbRestoreSimple(*restore, *destination)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("Restore was finished", *restore, "to", *destination)
		if change_master != "" {
			fmt.Printf("In order to start replication execute on slave:\n%s", change_master)
		}
	}
}
