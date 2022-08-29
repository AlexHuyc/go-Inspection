package config

import "time"

type Config struct {
	App                  []string      `ini:"app"`
	Polling              time.Duration `ini:"polling"`
	Sshdsvc              string        `ini:"sshdsvc"`
	Sshdpsst             string        `ini:"sshdpsst"`
	Mysqldsvc            string        `ini:"mysqldsvc"`
	Mysqldpsst           string        `ini:"mysqldpsst"`
	Grafanadsvc          string        `ini:"grafanasvc"`
	Grafanapsst          string        `ini:"grafanadpsst"`
	Nodemanagersvc       string        `ini:"nodemanagersvc"`
	Nodemanagerpsst      string        `ini:"nodemanagerpsst"`
	Resourcemmanagersvc  string        `ini:"resourcemmanagersvc"`
	Resourcemmanagerpsst string        `ini:"resourcemmanagerpsst"`
	Namenodesvc          string        `ini:"namenodesvc"`
	Nanenodepsst         string        `ini:"nanenodepsst"`
	Datanodesvc          string        `ini:"datanodesvc"`
	Datanodepsst         string        `ini:"datanodepsst"`
	Sentrysvc            string        `ini:"sentrysvc"`
	Sentrypsst           string        `ini:"sentrypsst"`
}
