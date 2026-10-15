<?php

require_once("database.php");

class PostgresDatabase extends Database {

	public function __construct($user="REDACTED", $password="REDACTED", $host="REDACTED") {
		parent::__construct($user, $password, $host);
	}

	protected function Connect() {
		if ($this->Connection == null) {
			
			#$this->Connection = pg_connect("host="REDACTED" port=5432 dbname=" . $dbname . " user="REDACTED" password="REDACTED"host="REDACTED" port=5432 dbname=postgres user="REDACTED" password="REDACTED"Es konnte keine Verbindung mit der Datenbank hergestellt werden. ".print_r($e, true));
			}
			if (isset($this->Config['timezone'])) {
				$this->Exec("set timezone='".$this->Config['timezone']."';");
			} else {
				$this->Exec("set timezone='Europe/Berlin';");
			}
		}
	}

	public function Disconnect() {
		pg_close($this->Connection);
		$this->Connection = null;
	}

	public function Commit() {
		# Function only nessesary for compatibility with oracle-based DB connection
		print("");
	}

	public function Rollback() {
		# Function only nessesary for compatibility with oracle-based DB connection
		print("");
	}

	public function FreeStatement() {
		pg_free_result($this->Statement);
	}

	public function Quote($value, $type='text') {
		$return = "";
		if ($type == "text") {
			$return = "'".str_replace("'", "''", $value)."'";
		} else if ($type == "integer") {
			if (!is_numeric($value)) {
				$return = "null";
			}
		}
		if ($value === '' || $value === null) {
			$return = 'null';
		}
		return $return;
	}

	public function ErrorToString($part="") {
		$str = "";
		switch ($part) {
			case "code": $str .= $this->LastError['code'];
				break;
			case "message": $str .= $this->LastError['message'];
				break;
			case "offset": $str .= $this->LastError['offset'];
				break;
			case "sql": $str .= $this->LastError['sqltext'];
				break;
			default: 
				if (array_key_exists('code', $this->LastError) && array_key_exists('message', $this->LastError)) {
					$str .= $this->LastError['code'].': '.$this->LastError['message'];
				}
		}
		return $str;
	}

	public function Query($sql, $bindCol="") {
		$this->LastQuery = $sql;
		if ($this->ReturnResult) {
			$result = array("Count" => 0, "Data" => array());
		} else {
			$result = false;
		}
		$this->LastError = array();
		if ($this->Connection == null) {
			$this->Connect();
		}
		#if ($this->Statement = oci_parse($this->Connection, $sql)) {
		$this->Statement = $sql;
		$result1 = pg_query($this->Connection, $sql);# or die("Query failed with Error: " . pg_last_error());
		if ($result1 ===FALSE) {
			$this->LastError['code'] = -1;
			$this->LastError['message'] = pg_last_error();
		} else {		
			$result = pg_fetch_all($result1);
			if ($result ===FALSE) {
				$this->LastError['code'] = -1;
				$this->LastError['message'] = pg_last_error($this->Connection);
			}
		}
		return $result;
	}

	public function Exec($sql) {
		$this->LastQuery = $sql;
		$result = 0; 						// Anzahl Zeilen des SQL Ergebnisses, im Fehlerfall false
		$this->LastError = array();
		if ($this->Connection == null) {
			$this->Connect();
		}
		$this->Statement = $sql;
		$result = pg_query($this->Connection, $sql);# or die("Query failed with Error: " . pg_last_error());
		if ($result ===FALSE) {
			$this->LastError['code'] = -1;
			$this->LastError['message'] = pg_last_error($this->Connection);
		}
		// $result = pg_fetch_all($result);
		// if ($result ===FALSE) {
		// 	$this->LastError['code'] = -1;
		// 	$this->LastError['message'] = pg_last_error();
		// }
		return $result;
	}

}
