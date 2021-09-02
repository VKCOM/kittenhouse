package cmd

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/Tirael666/kittenhouse/core/destination"
)

func TestRemoveComments(t *testing.T) {
	example := `
	# example
#and another
	of config
#and another`

	expected := `of config`

	if res := commentRegex.ReplaceAllString(example, ""); strings.TrimSpace(res) != strings.TrimSpace(expected) {
		t.Fatalf("Wrong remove comments: got '%s', expected '%s'", res, expected)
	}
}

func mapDiff(a, b destination.Map, aName, bName string) (diff []string) {
	for k, av := range a {
		bv, ok := b[k]
		if !ok {
			diff = append(diff, fmt.Sprintf("%s is missing key %s", bName, k))
			continue
		}

		if av.Default != bv.Default {
			diff = append(diff, fmt.Sprintf("%s[%s].Default != %s[%s].Default (%v != %v)", aName, k, bName, k, av.Default, bv.Default))
		}

		if !reflect.DeepEqual(av.Servers, bv.Servers) {
			diff = append(diff, fmt.Sprintf("%s[%s].Servers != %s[%s].Servers (%+v != %+v)", aName, k, bName, k, av.Servers, bv.Servers))
		}

		if !reflect.DeepEqual(av.Tables, bv.Tables) {
			diff = append(diff, fmt.Sprintf("%s[%s].Tables != %s[%s].Tables (%+v != %+v)", aName, k, bName, k, av.Tables, bv.Tables))
		}
	}

	for k := range b {
		if _, ok := a[k]; !ok {
			diff = append(diff, fmt.Sprintf("%s is missing key %s", aName, k))
		}
	}

	return diff
}

func TestSimpleConfig(t *testing.T) {
	m, err := parseConfig(bytes.NewBufferString(`* 127.0.0.1`))
	if err != nil {
		t.Fatalf("Could not parse config: %s", err.Error())
	}

	expected := destination.Map{
		"127.0.0.1:8123": &destination.Setting{
			Default: true,
			Servers: []destination.Server{{HostPort: "127.0.0.1:8123"}},
		},
	}

	if diff := mapDiff(m, expected, "parsed", "expected"); len(diff) > 0 {
		t.Errorf("Maps are different:\n%s", strings.Join(diff, "\n"))
	}
}

func TestParseConfig(t *testing.T) {
	m, err := parseConfig(bytes.NewBufferString(`
@target_port 3304;
# Forward all unknown ;tables to default host
* default;
table1 db1;
table2 db1;
table3 =db3*100
=db4*50;;table4  =db3*100  =db4*50;
`))

	expected := destination.Map{
		"default": &destination.Setting{
			Default: true,
			Servers: []destination.Server{{HostPort: "default:3304"}},
		},
		"db1": &destination.Setting{
			Tables:  []string{"table1", "table2"},
			Servers: []destination.Server{{HostPort: "db1:3304"}},
		},
		"=db3*100 =db4*50": &destination.Setting{
			Tables: []string{"table3", "table4"},
			Servers: []destination.Server{
				{HostPort: "db3:3304", Weight: 100},
				{HostPort: "db4:3304", Weight: 50},
			},
		},
	}

	if err != nil {
		t.Fatalf("Could not parse config: %s", err.Error())
	}

	if diff := mapDiff(m, expected, "parsed", "expected"); len(diff) > 0 {
		t.Errorf("Maps are different:\n%s", strings.Join(diff, "\n"))
	}
}

func TestBadParseConfig(t *testing.T) {
	needError := func(conf, substr string) {
		_, err := parseConfig(bytes.NewBufferString(conf))
		if err == nil {
			t.Errorf("Expected error for config %s", conf)
		} else if !strings.Contains(err.Error(), substr) {
			t.Errorf("Error '%s' does not contain '%s' for config %s", err.Error(), substr, conf)
		}
	}

	needError("", "No default section")
	needError("@target_port 0;\n* default;\n", "must be in range")
	needError("@target_port 65536;\n* default;\n", "must be in range")
	needError("* default extra columns;\n", `Expected '=' as first character`)
	needError("* =*100 =db2*50;\n", `Expected format '=host*weight'`)
	needError("* =db1* =db2*50;\n", `Expected format '=host*weight'`)
	needError("* =db1*N =db2*50;\n", `Could not parse weight`)
	needError("* =db1**100 =db2*50;\n", `Could not parse weight`)
	needError("* default;\n* another_default;", "Duplicate default")
	needError("@something 0;\n* default;\n", "Unsupported option")
	needError("* default;\ntable1 db1;\ntable1 db2;", "Duplicate table")
	needError("* default;\ntable1 default;\ntable1 db2;", "cannot have any custom tables")
}
