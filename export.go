package pbreplication

import (
	"encoding/json"
	"fmt"

	"github.com/pocketbase/pocketbase/core"
)

// exportRecord serializes a just-written record into a wire payload.
// It re-fetches the record within the current transaction so the
// exported map always contains ALL columns (an in-flight record may be
// flagged to export only the changed fields).
func exportRecord(app core.App, rec *core.Record) (json.RawMessage, map[string][]string, error) {
	col := rec.Collection()

	fresh, err := app.FindRecordById(col, rec.Id)
	if err != nil {
		return nil, nil, fmt.Errorf("export refetch %s/%s: %w", col.Name, rec.Id, err)
	}

	data, err := fresh.DBExport(app)
	if err != nil {
		return nil, nil, fmt.Errorf("export %s/%s: %w", col.Name, rec.Id, err)
	}

	payload, err := json.Marshal(data)
	if err != nil {
		return nil, nil, fmt.Errorf("export marshal %s/%s: %w", col.Name, rec.Id, err)
	}

	var files map[string][]string
	for _, f := range col.Fields {
		if f.Type() != core.FieldTypeFile {
			continue
		}
		names := fresh.GetStringSlice(f.GetName())
		if len(names) == 0 {
			continue
		}
		if files == nil {
			files = map[string][]string{}
		}
		files[f.GetName()] = names
	}

	return payload, files, nil
}

// exportCollectionJSON serializes a collection for replication.
//
// PocketBase's Collection.MarshalJSON intentionally blanks all token
// secrets (authToken, fileToken, ... and OAuth2 client secrets). For
// replication we must ship them: every node needs the SAME secrets or
// JWTs issued by one node would not validate on the others. The wire is
// only readable by cluster members (HMAC + documented "cluster secret =
// full access" model).
func exportCollectionJSON(col *core.Collection) (json.RawMessage, error) {
	raw, err := json.Marshal(col)
	if err != nil {
		return nil, err
	}
	if !col.IsAuth() {
		return raw, nil
	}

	var data map[string]any
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, err
	}

	setSecret := func(key, secret string) {
		if m, ok := data[key].(map[string]any); ok && secret != "" {
			m["secret"] = secret
		}
	}
	setSecret("authToken", col.AuthToken.Secret)
	setSecret("fileToken", col.FileToken.Secret)
	setSecret("passwordResetToken", col.PasswordResetToken.Secret)
	setSecret("emailChangeToken", col.EmailChangeToken.Secret)
	setSecret("verificationToken", col.VerificationToken.Secret)

	if o, ok := data["oauth2"].(map[string]any); ok {
		if provs, ok := o["providers"].([]any); ok {
			for i := range provs {
				pm, ok := provs[i].(map[string]any)
				if !ok {
					continue
				}
				name, _ := pm["name"].(string)
				for _, p := range col.OAuth2.Providers {
					if p.Name == name && p.ClientSecret != "" {
						pm["clientSecret"] = p.ClientSecret
					}
				}
			}
		}
	}

	// map re-marshal sorts keys -> deterministic output for the
	// content-hash idempotence check
	return json.Marshal(data)
}

// applyPayload writes a wire payload onto a record using the proper
// setter per field type:
//   - password: SetRaw passes the remote hash through unchanged
//   - autodate: SetRaw preserves the remote created/updated timestamps
//   - anything else: Set, so field setters normalize the JSON-decoded
//     values (file/relation/select slices, json blobs, geo points, ...)
func applyPayload(rec *core.Record, payload json.RawMessage) error {
	var data map[string]any
	if err := json.Unmarshal(payload, &data); err != nil {
		return fmt.Errorf("payload unmarshal: %w", err)
	}

	for _, f := range rec.Collection().Fields {
		name := f.GetName()
		if name == core.FieldNameId {
			continue // forced by the applier
		}
		v, ok := data[name]
		if !ok {
			continue
		}
		switch f.Type() {
		case core.FieldTypePassword:
			if s, ok := v.(string); ok && s != "" {
				rec.SetRaw(name, s)
			}
		case core.FieldTypeAutodate:
			rec.SetRaw(name, v)
		default:
			rec.Set(name, v)
		}
	}
	return nil
}
