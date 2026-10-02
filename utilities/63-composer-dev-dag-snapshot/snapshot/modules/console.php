<?php
define("STATS_MEM_START", memory_get_usage(false));
define("STATS_TIME_START", microtime(true));

ini_set("log_errors", 1);
if (strtoupper(substr(php_uname('s'), 0, 3)) === 'WIN') {
	ini_set("error_log", "../errorlog.txt");
} else {
	ini_set("error_log", "errorlog.txt");
}

error_reporting(E_ALL & ~(E_STRICT|E_NOTICE|E_DEPRECATED));
set_time_limit(0);

if (!defined("BASE_DIR")) {
	define("BASE_DIR", dirname(__FILE__) . "/..");
}

require_once("db-postgres.php");
define("LOG_CONNECT", "A");
define("LOG_HINT", "H");
define("LOG_WARN", "W");
define("LOG_ERROR", "F");
define("LOG_FILE", "L");

#function error_handler($level, $message, $file, $line, $context) {
function error_handler($level, $message, $file, $line) { #, $context) {
	$message = trim($message);
	if ($message == 'ssh2_exec(): Unable to request command execution on remote host') {
		return true;
	}
	$typ = '';
	switch ($level) {
		case E_ERROR: $typ = 'Fatal';
			break;
		case E_WARNING: $typ = 'Warnung';
			break;
		case E_PARSE: $typ = 'Parse';
			break;
		case E_NOTICE: $typ = 'Hinweis';
			break;
		default: $typ = 'Anderes Level '.$level;
	}
	error_log($typ.': Zeile '.$line.' in '.$file."\n".$message);
	return true;
}

set_error_handler('error_handler', ini_get('error_reporting'));

class Console {

	protected $Args = array();
	protected $ArgMap = array();
	protected $ArgsDef = array(
		"Shell" => false,
		'Test' => false,
		'NoMail' => false,
		'SchedSID' => 0,
		'SchedPID' => 0,
		'SchedSIDOverride' => null,
		'SchedPIDOverride' => null
	);
	protected $ArgDefMap = array(
		"shell" => "Shell",
		'test' => 'Test',
		'nomail' => 'NoMail',
		'schedsid' => 'SchedSID',
		'schedpid' => 'SchedPID',
		'schedsidoverride' => 'SchedSIDOverride',
		'schedpidoverride' => 'SchedPIDOverride'
	);
	protected $ProcessID = array();
	protected $Config = array();
	protected $ScriptName = "console.php";
	protected $IncludeSmarty = true;
	protected $IncludeTwig = false;
	protected $loader = null;
	protected $twig = null;
	protected $LogDB = null;
	public $LogUsage = false;
	protected $email_charset_txt = "iso-8859-1";
	protected $email_charset_html = "iso-8859-1";
	protected $email_charset_recp = "iso-8859-1";
	protected $email_eol = "\n";
	private $OracleStats = null;


	public function __construct(){
		global $argc, $argv;
		$this->ParseArguments();
		$this->GetProcess();
		require(__DIR__ . "/../config-global.php");
		$this->Config = $Config;
		if ($this->IncludeSmarty) {
			} else if ($this->IncludeTwig) {
			#require_once(__DIR__ . "/../twig/Autoloader.php");
			Twig_Autoloader::register();
			$this->loader = new Twig_Loader_Filesystem(BASE_DIR . '/templates');
			$this->twig = new Twig_Environment($this->loader, array(
				'cache' => BASE_DIR . '/templates_c',
			));
		}
		if (!(isset($this->Args["Shell"]) && $this->Args["Shell"])) {
			try {
				$this->LogDB = new PostgresDatabase($this->Config['Database'][2]['User'], $this->Config['Database'][2]['Password'], $this->Config['Database'][2]['Host']);
				register_shutdown_function(array(&$this, 'Shutdown'));
				if (isset($this->Config['timezone'])) {
					$this->LogDB->Exec("set timezone='".$this->Config['timezone']."';");
				} else {
					$this->LogDB->Exec("set timezone='Europe/Berlin';");
				}
				if ($this->LogUsage) {
					#$result = $this->getOracleStats();
					#$this->OracleStats = $result;
				}
			} catch(Exception $e) {
				die($e->getMessage());
				$this->LogDB = null;
				error_log($e->getMessage());
				file_put_contents("db-connection.txt", date("d.m.Y H:i:s\n") . $e->getMessage() . "\n", FILE_APPEND);
			}
		}
	}

	private function ParseArguments(){
		global $argc, $argv;

		$this->Args = $this->ArgsDef;
		# Default Parameter an die des Scriptes anh�ngen
		$this->ArgMap += $this->ArgDefMap;
		for ($i=1; $i<$argc; $i++){
			$p = $argv[$i];
			# Paramter fangen mit - an
			if ($p[0] == "-"){
				# - aus dem Parameternamen entfernen
				$p = substr($p, 1);
				# Parameter nur ber�cksichtigen wenn dieser in ArgMap definiert ist
				if (isset($this->ArgMap[$p])){
					# Falls das der letzte Parameter ist oder der Folgeparameter f�ngt mit einem - an, dann ist das ein Flagparameter
					if (!isset($argv[$i+1]) || $argv[$i+1][0] == "-" || $argv[$i+1] == 'true' || $argv[$i+1] == '') {
						$this->Args[$this->ArgMap[$p]] = true;
					} else if ($argv[$i+1] == 'false') {
						$this->Args[$this->ArgMap[$p]] = false;
					} else { # Ansonsten wird der Wert vom Folgeparameter gesetzt
						$this->Args[$this->ArgMap[$p]] = $argv[$i+1];
					}
				}
			}
		}
	}

	private function GetProcess(){
#		if (!$this->Args["Shell"]){
#			$cmd = "ps -C ".$this->ScriptName." -o pid=";
#			echo $cmd."\n";
#			exec($cmd, $this->ProcessID);
#			$cmd = "ps -C ".$this->ScriptName." -o ppid=";
#			echo $cmd."\n";
#			exec($cmd, $this->ProcessID);
		if (!$this->isWindows()) {
			if (is_numeric($this->Args["SchedSIDOverride"]) && is_numeric($this->Args["SchedPIDOverride"])) {
				$this->ProcessID[0] = $this->Args["SchedSIDOverride"];    # Eigene Prozess ID
				$this->ProcessID[1] = $this->Args["SchedPIDOverride"];    # Eltern Prozess ID
			} else {
				$this->ProcessID[0] = posix_getpid();    # Eigene Prozess ID
				$this->ProcessID[1] = posix_getppid();    # Eltern Prozess ID
			}
		} else {
			$this->ProcessID[0] = 0;
			$this->ProcessID[1] = 0;
		}

			# F�r Kompatibilit�t bleibt der alte Index erstmal erhalten
			$this->ProcessID['own'] = $this->ProcessID[0];
			$this->ProcessID['parent'] = $this->ProcessID[1];
#		}
	}

	protected function LogConnect($message){
		$this->Log(LOG_CONNECT, $message);
	}

	protected function LogHint($message){
		$this->Log(LOG_HINT, $message);
	}

	protected function LogWarning($message){
		$this->Log(LOG_WARN, $message);
	}

	protected function LogError($message){
		$this->Log(LOG_ERROR, $message);
	}

	protected function LogFile($filepath){
		$this->Log(LOG_FILE, $filepath);
	}

	protected function InsertLog($schedulerid, $prozessid, $pid, $ppid, $typ, $zeile, $text) {
		$sql = 'insert into prozessmonitordb.prozesslog (schedulerid, prozessid, pid, typ, zeile, text)';
		if ($schedulerid === '' && $prozessid === '') {
			$sql .= ' select schedulerid, letzteprozessid';
			$sql .= ', '.$pid;
			$sql .= ', '.$this->LogDB->Quote($typ);
			$sql .= ', '.$zeile;
			$sql .= ', '.$this->LogDB->Quote($text);
			$sql .= ' from prozessmonitordb.prozessscheduler';
			$sql .= ' where pid = '.$ppid;
		} else {
			$sql .= ' values (';
			$sql .= $schedulerid;
			$sql .= ', '.$prozessid;
			$sql .= ', '.$pid;
			$sql .= ', '.$this->LogDB->Quote($typ);
			$sql .= ', '.$zeile;
			$sql .= ', '.$this->LogDB->Quote($text);
			$sql .= ')';
		}
		$result = $this->LogDB->Exec($sql);
		if ($result === false || $result == 0) {
			if ($result === false) {
				error_log("console->InsertLog: " . $this->LogDB->ErrorToString());
			} else {
				error_log('Es wurden keine Logeintr�ge geschrieben.');
				error_log($sql);
			}
		}
	}

	protected function Log($type, $message) {
		if (isset($this->Args["Shell"]) && $this->Args["Shell"]) {
			switch ($type) {
				case LOG_HINT: echo "Hinweis: ";
				break;
				case LOG_WARN: echo "Warnung: ";
				break;
				case LOG_ERROR: echo "Fehler: ";
				break;
				case LOG_CONNECT: echo "Anmeldung: ";
				break;
				default: echo "Meldung: ";
			}
			echo $message."\n";
		}
		if ($type == LOG_ERROR) {
			error_log($message);
		}
		if ($this->LogDB) {
			if ($type != LOG_FILE) {
				if ($type == LOG_ERROR) {
					$typ = "Fehler";
				} else if ($type == LOG_WARN) {
					$typ = "Warnung";
				} else if ($type == LOG_CONNECT) {
					$typ = "Anmeldung";
				} else {
					$typ = "Hinweis";
				}
				$this->InsertLog('', '', $this->ProcessID['own'], $this->ProcessID['parent'], $typ, 0, $message);
			} else {
#				$message = str_replace('&', "'||chr(38)||'", $message);
				# VPID = Vater ID oder PPID
				# PID = Eigene ID
				$zeile = 0;
				$typ = "Logfile";
				$this->InsertLog('', '', $this->ProcessID['own'], $this->ProcessID['parent'], $typ, $zeile, $message);
				if (file_exists($message)) {
					$fh = fopen($message, 'r');
					while (($buffer = fgets($fh)) !== false) {
						$zeile++;
						$this->InsertLog('', '', $this->ProcessID['own'], $this->ProcessID['parent'], $typ, $zeile, $buffer);
					}
					fclose($fh);
					unlink($message);
				}
#				exec("./ProzessLOG.sh -VPID ".$this->ProcessID['parent']." -PID ".$this->ProcessID['own']." -a ".$type." -i \"".$message."\"");
			}
			$this->LogDB->Commit();
		}
	}

	protected function ObjectToArray($d) {
		if (is_object($d)) {
			$d = get_object_vars($d);
		}
		if (is_array($d)) {
			return array_map(array($this, "ObjectToArray"), $d);
		}
		else {
			return $d;
		}
	}

	protected function sendEMail($subject, $recipientsList, $template, $data=null, $highPriority=false, $attachFile=null, $attachFilename=null, $attachType="text/plain"){
		$recipients = array();
		if (is_string($recipientsList)) {
			$recipients["to"] = $recipientsList;
		} else if (is_array($recipientsList) && !isset($recipientsList["to"])) {
			$recipients["to"] = $recipientsList;
		} else if (empty($recipientsList)) {
			throw new Exception("Es wurden keine Empfänger definiert.");
		} else {
			# Müsste ein Array sein mit Key "to"
			$recipients = $recipientsList;
		}

		$smarty = new Smarty();
		if (!empty($data)) {
			$smarty->assign("data", $data);
		}
		$mime = new Mail_mime(array(
			"text_charset" => $this->email_charset_txt,
			"html_charset" => $this->email_charset_html,
			"eol" => $this->email_eol
		));

		$headers = array(
			"From"		=> $this->Config["Mail"]["From"],
			"To"		=> "",
			"Cc"		=> "",
			"Bcc"		=> "",
			"Subject"	=> $this->Config["Mail"]["SubjectPrefix"] . $subject
		);
		if ($highPriority) {
			$headers['X-Priority'] = '1';
			$headers['X-MSMail-Priority'] = 'High';
			$headers['Importance'] = 'High';
		}
		#var_dump($smarty->getTemplateDir());
		$dir = $smarty->getTemplateDir();
		$usedTemplate = false;
		if (file_exists($dir[0] . $template . ".tpl")) {
			$usedTemplate = true;
			$mail_txt = $smarty->fetch($template . ".tpl");
			#var_dump($mail_txt);
			$mime->setTXTBody($mail_txt);
		}
		if (file_exists($dir[0] . $template . ".html")) {
			$usedTemplate = true;
			$mail_html = $smarty->fetch($template . ".html");
			#var_dump($mail_html);
			$mime->setHTMLBody($mail_html);
		}
		if (!$usedTemplate) {
			# Wenn $template nicht als Datei vorhanden dann den Inhalt als Body verwenden
			$mime->setTXTBody($template);
		}
		if (!empty($attachFile) && file_exists($attachFile)) {
			$mime->addAttachment($attachFile, $attachType, $attachFilename);
		}

		$mail = Mail::factory("smtp", array(
			"auth" => $this->Config["Mail"]["Authentication"],
			"host" => $this->Config["Mail"]["Host"],
			"username" => $this->Config["Mail"]["User"],
			"password" => $this->Config["Mail"]["Password"],
			"debug" => $this->Config["Mail"]["Debug"],
			"port" => $this->Config["Mail"]["Port"],
			"socket_options" => $Config["Mail"]["SSLOptions"]
		));
		$recipients["all"] = "";
		if (!empty($recipients["to"])) {
			if (is_array($recipients["to"])) {
				$recipients["to"] = implode(',', $recipients["to"]);
			}
			$recipients["all"] = $recipients["to"];
		}
		if (!empty($recipients["cc"])) {
			if (is_array($recipients["cc"])) {
				$recipients["cc"] = implode(',', $recipients["cc"]);
			}
			$recipients["all"] .= "," . $recipients["cc"];
		}
		if (!empty($recipients["bcc"])) {
			if (is_array($recipients["bcc"])) {
				$recipients["bcc"] = implode(',', $recipients["bcc"]);
			}
			$recipients["all"] .= "," . $recipients["bcc"];
		}
		if ($this->Args["Test"]) {
			$this->LogHint("Eigentliche(r) E-Mail Empf�nger: " . $recipients["to"]);
			$recipients["to"] = $this->Config["Mail"]["SenderMail"];
			$recipients["cc"] = "";
			$recipients["bcc"] = "";
		}
		$headers["To"] = $recipients["to"];
		$headers["Cc"] = $recipients["cc"];
		$headers["Bcc"] = $recipients["bcc"];
		foreach ($headers as $name => $value) {
			$headers[$name] = $mime->encodeHeader($name, $value, $this->email_charset_recp, "quoted-printable");
		}
		$mail_body = $mime->get();
		$mail_header = $mime->headers($headers, true);
		$res = $mail->send($mime->encodeHeader("to", $recipients["all"], $this->email_charset_recp, "quoted-printable"), $mail_header, $mail_body);
		if (PEAR::isError($res)) {
			$this->LogError($recipients["all"] . $res->getMessage());
		} else {
			$this->LogHint("Mail wurde gesendet an " . $recipients["to"]);
		}
		unset($smarty);
	}

	/**
	 * Erzeugt aus dem Array ein insert SQL. Das Array muss als key den Spaltennamen haben und als Wert den entsprechenden Wert.
	 * Als Wert ist es auch m�glich einen SQL zu nutzen oder eine Formel/Funktion.
	 * @param string $table
	 * @param array $data
	 * @return string SQL
	 */
	protected function buildInsertSQL($table, $data) {
		$sql = "insert into " . $table;
		$sql .= "\n (\n" . implode("\n, ", array_keys($data)) . "\n)";
		$sql .= " values (\n" . implode("\n, ", array_values($data)) . "\n)";
		return $sql;
	}

	/**
	 * Erzeugt aus dem Array ein update SQL. Das Array muss als key den Spaltennamen haben und als Wert den entsprechenden Wert.
	 * Als Wert ist es auch m�glich einen SQL zu nutzen oder eine Formel/Funktion.
	 * @param string $table
	 * @param array $data
	 * @param string|array $where Wenn es ein Array ist wird alles mit AND verkn�pft
	 * @return string
	 */
	protected function buildUpdateSQL($table, $data, $where = null) {
		$sql = "update " . $table;
		$sql .= " set \n";
		$sets = array();
		foreach ($data as $key => $value) {
			$sets[] = $key . " = " . $value;
		}
		$sql .= implode(",\n", $sets);
		if (!empty($where)) {
			$sql .= "\n where \n";
			if (is_array($where)) {
				$wheres = array();
				foreach ($where as $key => $value) {
					$wheres[] = $key . " = " . $value;
				}
				$sql .= implode("\n and ", $wheres);
			} else if (is_string($where)) {
				$sql .= $where;
			}
		}
		return $sql;
	}

	/**
	 * Erzeugt aus dem Array ein delete SQL. Das Array wird ignoriert und es kann null �bergeben werden. Es dient nur einer einheitlichen
	 * Funktionssignatur.
	 * @param string $table
	 * @param array $data Wird ignoriert und kann daher null sein
	 * @param string|array $where Wenn es ein Array ist wird alles mit AND verkn�pft
	 * @return string
	 */
	protected function buildDeleteSQL($table, $data, $where = null) {
		$sql = "delete from " . $table;
		if (!empty($where)) {
			$sql .= "\n where \n";
			if (is_array($where)) {
				$wheres = array();
				foreach ($where as $key => $value) {
					$wheres[] = $key . " = " . $value;
				}
				$sql .= implode("\n and ", $wheres);
			} else if (is_string($where)) {
				$sql .= $where;
			}
		}
		return $sql;
	}

	/**
	 * Erzeugt aus dem Array ein einfaches Select.
	 *
	 * @param      $table
	 * @param      $data
	 * @param null $where
	 *
	 * @return string
	 */
	protected function buildSelectSQL($table, $data, $where = null, $columns = null) {
		$sql = "select * from " . $table;
		if (!empty($columns)) {
			$sql = "select " . implode(", ", $columns) . " from " . $table;
		}
		if (!empty($where)) {
			$sql .= "\n where \n";
			if (is_array($where)) {
				$wheres = array();
				foreach ($where as $key => $value) {
					if ($value == "NULL" || $value == "null") {
						$wheres[] = $key . " is null";
					} else {
						$wheres[] = $key . " = " . $value;
					}
				}
				$sql .= implode("\n and ", $wheres);
			} else if (is_string($where)) {
				$sql .= $where;
			}
		}
		return $sql;
	}

	public function isWindows() {
		return (strtoupper(substr(php_uname('s'), 0, 3)) === 'WIN');
	}

	public function getOracleStats() {
		$statids = array(
			35,		# session pga memory
			36,		# session pga memory max
			17,		# CPU used by this session in 10s of milliseconds
			18,		# DB time
			83,		# physical read bytes
			52,		# physical read total bytes
			77,		# logical read bytes from cache
			96,		# physical write bytes
			55,		# physical write total bytes
			178		# redo size in bytes
		);
/*		$sql = " 	select s.sid, s.value, S.STATISTIC# as statid, n.name";
		$sql .= ' 	from v$sesstat s, V$STATNAME n';
		$sql .= " 	where S.STATISTIC# = N.STATISTIC#";
		$sql .= " 	and s.sid = sys_context('USERENV','SID')";
		$sql .= " 	and s.value > 0";
		$result = $this->LogDB->Query($sql);
		file_put_contents(BASE_DIR . "/oracle-metrics.txt", print_r($result, true), FILE_APPEND);
		$sql = "select *";
		$sql .= " from (";
		$sql .= " 	select s.sid, s.value, S.STATISTIC# as statid";
		$sql .= ' 	from v$sesstat s, V$STATNAME n';
		$sql .= " 	where S.STATISTIC# = N.STATISTIC#";
		$sql .= " 	and s.sid = sys_context('USERENV','SID')";
		$sql .= " )";
		$sql .= " pivot (";
		$sql .= " 	sum(value)";
		$sql .= " 	for statid in (" . implode(",", $statids) . ")";
		$sql .= " )";
		$result = $this->LogDB->Query($sql);
		return $result['Data'][0];
*/
	}

	/**
	 * Linux Ausgabe von getrusage
	 * [ru_oublock] => 0			block output operations
	 * [ru_inblock] => 0			block input operations
	 * [ru_msgsnd] => 0				IPC messages sent
	 * [ru_msgrcv] => 0				IPC messages received
	 * [ru_maxrss] => 20652			maximum resident set size
	 * [ru_ixrss] => 0				integral shared memory size
	 * [ru_idrss] => 0				integral unshared data size
	 * [ru_minflt] => 5839			page reclaims (soft page faults)
	 * [ru_majflt] => 0				page faults (hard page faults)
	 * [ru_nsignals] => 0			signals received
	 * [ru_nvcsw] => 43				voluntary context switches
	 * [ru_nivcsw] => 7				involuntary context switches
	 * [ru_nswap] => 0				number of swaps
	 * [ru_utime.tv_usec] => 24001	user CPU time used (microseconds)
	 * [ru_utime.tv_sec] => 0		user CPU time used (seconds)
	 * [ru_stime.tv_usec] => 28001	system CPU time used (microseconds)
	 * [ru_stime.tv_sec] => 0		system CPU time used (seconds)
	 * F�r die echte Nutzung an CPU muss man user und system CPU Time zusammenrechnen.
	 */
	public function getScriptStats() {
		$used = array(
			'script' => $this->ScriptName,
			'schedsid' => $this->Args['SchedSID'],
			'schedpid' => $this->Args['SchedPID'],
			'mem_peak_real' => memory_get_peak_usage(true),
			'mem_peak' => memory_get_peak_usage(false),
			'mem_real' => memory_get_usage(true),
			'mem' => memory_get_usage(false),
			'mem_start' => STATS_MEM_START,
			'runtime' => (microtime(true) - STATS_TIME_START),
			'rusage' => ( $this->isWindows() ? array() : getrusage() ),
			'oracle_start' => $this->OracleStats,
			'oracle_end' => $this->getOracleStats()
		);
		// F�r die CPU Nutzung beides, User und System Zeit zusammenrechnen, in ms Umwandeln
		#$used['cputime_usec'] = $used['rusage']['ru_utime.tv_usec'] + $used['rusage']['ru_stime.tv_usec'];
		#$used['cputime_msec'] = $used['cputime_usec'] / 1000;
		#$used['cputime_sec'] = $used['cputime_msec'] / 1000;
		#$used['oracle_dbtime'] = $used['oracle_end']['18'] / 100;
		#$used['oracle_dbmem'] = $used['oracle_end']['36'];
		#$used['oracle_dbread'] = $used['oracle_end']['83'];
		#$used['oracle_dbwrite'] = $used['oracle_end']['96'];
		#$used['oracle_dbcache'] = $used['oracle_end']['77'];
		#$used['oracle_dbredosize'] = $used['oracle_end']['178'];
#		file_put_contents(BASE_DIR . "/usage.txt", print_r($used, true), FILE_APPEND);
		$sql = $this->buildInsertSQL("prozessstats", array(
			'schedulerid' => $this->Args['SchedSID'],
			'prozessid' => $this->Args['SchedPID'],
			'cputime' => str_replace(",", ".", $used['cputime_sec']),
			'phpmempeak' => $used['mem_peak_real'],
			'phpmemstart' => $used['mem_start'],
			'runtime' => str_replace(",", ".", $used['runtime']),
			'dbtime' => str_replace(",", ".", $used['oracle_dbtime']),
			'dbmem' => $used['oracle_dbmem'],
			'dbbytesread' => $used['oracle_dbread'],
			'dbbyteswrite' => $used['oracle_dbwrite'],
			'dbbytescache' => $used['oracle_dbcache'],
			'dbredosize' => $used['oracle_dbredosize']
		));
		$this->LogDB->Exec($sql);
		$this->LogDB->Commit();
	}

	public function Shutdown() {
		if ($this->LogUsage) {
			$this->getScriptStats();
		}
	}

	public function Run(){
	}

}
