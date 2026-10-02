<?php

class Database {

/**
 * Ressource f�r die Datenbankenverbindung
 * @var ressource
 */
public $Connection = null;
/**
 * Anzahl Fehler
 * @var integer
 */
public $ErrorCount = 0;
/**
 * Benutzer f�r die Verbindung
 * @var string
 */
public $User = "REDACTED";
/**
 * Passwort f�r die Verbindung
 * @var string
 */
public $Password = "REDACTED";
/**
 * Server f�r die Verbindung
 * @var string
 */
public $Host = "REDACTED";
/**
 * Fehlermeldung des letzten Fehlers
 * @var string
 */
public $LastError = "";
/**
 * Query die zuletzt ausgef�hrt wurde
 * @var string
 */
public $LastQuery = '';
/**
 * Ressource zur preparierten Abfrage f�r ein Exec oder Query
 * @var ressource
 */
public $Statement = null;
/**
 * Flag ob nach einem Exec einer Query automatisch ein Commit gemacht werden soll
 * @var boolean
 */
public $AutoCommit = false;
/**
 * Flag ob am Ende eines Exec oder Query das Statement freigegeben werde soll
 * @var boolean
 */
public $AutoFree = true;
/**
 * Flag ob das Ergebnis zur�ckgeliefert werden soll oder nur ausgef�hrt wird. Bei nur Ausf�hren kann das Ergebnis manuell �ber das Statement abgefragt werden
 * @var boolean
 */
public $ReturnResult = true;
/**
 * Flag ob nach einem Exec einer Query automatisch die Verbindung geschlossen werden soll
 * @var boolean
 */
public $AutoClose = false;

	public function __construct($user="REDACTED", $password="REDACTED", $host="REDACTED") {
		if ($user != "")
			$this->User = $user;
		if ($password != "")
			$this->Password = $password;
		if ($host != "")
			$this->Host = $host;
		$this->CheckSettings();
	}

	/**
	 * Verbindungsdaten pr�fen
	 */
	protected function CheckSettings() {
		if ($this->User == "") {
			throw new Exception('Der Parameter USER muss gefuellt sein.');
		}

		if ($this->Password == "") {
			throw new Exception('Der Parameter PASSWORD muss gefuellt sein.');
		}

		if ($this->Host == "") {
			throw new Exception('Der Parameter HOST  muss gefuellt sein.');
		}
	}

	/**
	 * Verbindung zur Datenbank mit den Verbindungsdaten aufbauen
	 */
	protected function Connect() {
	}

	/**
	 * Verbindung zur Daten trennen
	 */
	protected function Disconnect() {
	}

	/**
	 * Automatisches Commiten in einer Transaktion aktivieren/deaktivieren
	 * @param bool $use Commiten aktivieren (true)/deaktivieren (false)
	 */
	public function AutoCommit($use) {
		$this->AutoCommit = ($use === true);
	}

	/**
	 * Automatisches freigeben des Statements aktivieren/deaktivieren
	 * @param bool $use Freigeben aktivieren (true)/deaktivieren (false)
	 */
	public function AutoFree($use) {
		$this->AutoFree = ($use === true);
	}

	/**
	 * R�ckgabe des kompletten Ergebnisses einer Abfrage aktivieren/deaktivieren
	 * @param bool $use R�ckgabe aktivieren (true)/deaktivieren (false)
	 */
	public function ReturnResult($use) {
		$this->ReturnResult = ($use === true);
		# Wenn das Ergebniss nicht zur�ckgeliefert werden soll das Automatische Freigeben des Statements ebenfalls deaktivieren damit es abgefragt werden kann
		if (!$this->ReturnResult)
			$this->AutoFree(false);
	}

	/**
	 * Query ausf�hren und betroffene Zeilen als Wert zur�ckliefern. Benutzen zb f�r INSERT, UPDATE, DELETE.
	 */
	public function Exec($sql) {
	}

	/**
	 * Query ausf�hren und Ergebnis einer Abfrage zur�ckliefern. Benutzen zb f�r SELECT.
	 */
	public function Query($sql) {
	}

	/**
	 * In einer Transaktion den aktuellen Stand Commiten.
	 */
	public function Commit() {
	}

	/**
	 * Stellt den Zustand bis zum letzten Commit wiederher.
	 */
	public function Rollback() {
	}

	/**
	 * Gibt den Speicher des Statements wieder frei
	 */
	public function FreeStatement() {
	}

	/**
	 * Generiert aus dem Fehler in LastError einen String der alle Daten enth�lt.
	 */
	public function ErrorToString() {
	}

	/**
	 * Escaped Zeichen die in einer Query f�r Fehler sorgen k�nnen
	 */
	public function Quote($value, $type='text') {
		return $value;
	}

}

?>