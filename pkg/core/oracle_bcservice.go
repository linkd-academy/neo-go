package core

import (
	"fmt"

	"github.com/nspcc-dev/neo-go/pkg/core/native"
	"github.com/nspcc-dev/neo-go/pkg/core/native/noderoles"
	"github.com/nspcc-dev/neo-go/pkg/smartcontract/manifest"
)

// SetOracle sets oracle module. It can safely be called on the running blockchain.
// To unregister Oracle service use SetOracle(nil).
func (bc *Blockchain) SetOracle(mod native.OracleService) {
	orc := bc.contracts.Oracle
	currentHF := bc.getCurrentHF()
	if mod != nil {
		orcMd := orc.HFSpecificContractMD(&currentHF)
		md, ok := orcMd.GetMethod(manifest.MethodVerify, -1)
		if !ok {
			panic(fmt.Errorf("%s method not found", manifest.MethodVerify))
		}
		mod.UpdateNativeContract(orcMd.NEF.Script, orc.GetOracleResponseScript(),
			orc.Hash, md.MD.Offset)
		keys, _, err := bc.GetDesignatedByRole(noderoles.Oracle)
		if err != nil {
			bc.log.Error("failed to get oracle key list")
			return
		}
		mod.UpdateOracleNodes(keys)
		reqs, err := bc.contracts.Oracle.GetRequests(bc.dao)
		if err != nil {
			bc.log.Error("failed to get current oracle request list")
			return
		}
		mod.AddRequests(reqs)
	}
	orc.Module.Store(&mod)
	bc.contracts.Designate.OracleService.Store(&mod)
}