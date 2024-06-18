package ton

import "testing"

func TestUnifyAddress(t *testing.T) {
	t.Log(unifyAddressToHuman("0:0D23E6778F5D9EA8D2A9BA584EBD1B191A65458D9C4C2A8DF38B21477F8F5B47"))
	t.Log(unifyAddressToHuman("0:33230d735153b65a47e0252f3a8f1364299edc94429cf64f44aa3d5a8d413663"))
	t.Log(unifyAddressToHuman("0:b15e023cb1ff1371d85dab7f1713f7be4b3edef384f9f4445a9815551c6d33c1"))
}
