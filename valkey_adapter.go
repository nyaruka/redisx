package redisx

import (
	"context"
	"fmt"

	"github.com/valkey-io/valkey-go"
)

// Pool interface that matches redigo's pool interface
type Pool interface {
	Get() Conn
}

// Conn interface that matches redigo's connection interface
type Conn interface {
	Do(commandName string, args ...interface{}) (reply interface{}, err error)
	Send(commandName string, args ...interface{}) error
	Close() error
}

// Script interface for Lua script execution
type Script interface {
	Do(c Conn, keysAndArgs ...interface{}) (interface{}, error)
}

// ValkeyPool wraps valkey-go client to provide pool-like interface similar to redigo
type ValkeyPool struct {
	client valkey.Client
}

// ValkeyConn wraps valkey-go client to provide connection-like interface similar to redigo
type ValkeyConn struct {
	client    valkey.Client
	ctx       context.Context
	pipelined []valkey.Completed
	multi     bool
}

// ValkeyScript wraps valkey-go Lua script functionality
type ValkeyScript struct {
	script   string
	numKeys  int
	lua      *valkey.Lua
}

// NewValkeyPool creates a new valkey pool wrapper
func NewValkeyPool(client valkey.Client) *ValkeyPool {
	return &ValkeyPool{client: client}
}

// Get returns a connection from the pool
func (p *ValkeyPool) Get() Conn {
	return &ValkeyConn{
		client:    p.client,
		ctx:       context.Background(),
		pipelined: nil,
		multi:     false,
	}
}

// Close closes the connection (no-op for valkey-go)
func (c *ValkeyConn) Close() error {
	// valkey-go handles connections internally
	return nil
}

// Do executes a Redis command
func (c *ValkeyConn) Do(cmd string, args ...interface{}) (interface{}, error) {
	// If we're in MULTI mode and this is EXEC, execute the transaction
	if c.multi && cmd == "EXEC" {
		if len(c.pipelined) == 0 {
			return []interface{}{}, nil // Empty transaction returns empty array
		}
		results := c.client.DoMulti(c.ctx, c.pipelined...)
		c.pipelined = nil
		c.multi = false
		
		// Return the results array - similar to redigo's behavior
		interfaceResults := make([]interface{}, len(results))
		for i, result := range results {
			if result.Error() != nil {
				return nil, result.Error()
			}
			interfaceResults[i] = result
		}
		return interfaceResults, nil
	}
	
	// If this is MULTI, start transaction mode
	if cmd == "MULTI" {
		c.multi = true
		c.pipelined = nil
		return "OK", nil
	}
	
	// Convert all args to strings for valkey command builder
	strArgs := make([]string, len(args))
	for i, arg := range args {
		strArgs[i] = fmt.Sprintf("%v", arg)
	}
	
	// Build command using valkey's command builder
	command := c.client.B().Arbitrary(cmd)
	for _, arg := range strArgs {
		command = command.Args(arg)
	}
	
	// If we're in multi mode, queue the command
	if c.multi {
		c.pipelined = append(c.pipelined, command.Build())
		return "QUEUED", nil
	}
	
	// Execute command immediately
	result := c.client.Do(c.ctx, command.Build())
	if result.Error() != nil {
		return nil, result.Error()
	}
	
	// Return the raw result - the caller will use helper functions to convert
	return result, nil
}

// Send queues a command for pipeline/transaction execution
func (c *ValkeyConn) Send(cmd string, args ...interface{}) error {
	// In multi mode, treat Send the same as Do
	if c.multi {
		_, err := c.Do(cmd, args...)
		return err
	}
	
	// For compatibility, just execute immediately if not in multi mode
	_, err := c.Do(cmd, args...)
	return err
}

// NewScript creates a new script similar to redis.NewScript
func NewScript(keyCount int, script string) Script {
	return &ValkeyScript{
		script:  script,
		numKeys: keyCount,
		lua:     nil, // Will be created when first used
	}
}

// Do executes the Lua script
func (s *ValkeyScript) Do(c Conn, keysAndArgs ...interface{}) (interface{}, error) {
	valkeyConn, ok := c.(*ValkeyConn)
	if !ok {
		return nil, fmt.Errorf("connection is not a ValkeyConn")
	}
	
	// Create Lua script if not already created
	if s.lua == nil {
		s.lua = valkey.NewLuaScript(s.script)
	}
	
	var keys []string
	var args []string
	
	if s.numKeys == -1 {
		// Dynamic key count - first argument is the number of keys
		if len(keysAndArgs) == 0 {
			return nil, fmt.Errorf("missing key count for dynamic script")
		}
		
		keyCount := int(keysAndArgs[0].(int))
		if len(keysAndArgs) < keyCount+1 {
			return nil, fmt.Errorf("insufficient arguments for dynamic script")
		}
		
		keys = make([]string, keyCount)
		for i := 0; i < keyCount; i++ {
			keys[i] = fmt.Sprintf("%v", keysAndArgs[i+1])
		}
		
		args = make([]string, len(keysAndArgs)-keyCount-1)
		for i := keyCount + 1; i < len(keysAndArgs); i++ {
			args[i-keyCount-1] = fmt.Sprintf("%v", keysAndArgs[i])
		}
	} else {
		// Fixed key count
		keys = make([]string, s.numKeys)
		args = make([]string, len(keysAndArgs)-s.numKeys)
		
		for i := 0; i < s.numKeys && i < len(keysAndArgs); i++ {
			keys[i] = fmt.Sprintf("%v", keysAndArgs[i])
		}
		
		for i := s.numKeys; i < len(keysAndArgs); i++ {
			args[i-s.numKeys] = fmt.Sprintf("%v", keysAndArgs[i])
		}
	}
	
	// Execute the script
	result := s.lua.Exec(valkeyConn.ctx, valkeyConn.client, keys, args)
	if result.Error() != nil {
		return nil, result.Error()
	}
	
	return result, nil
}

// Helper functions to convert valkey responses to redigo-compatible types

// String converts a valkey result to string (similar to redis.String)
func String(result interface{}, err error) (string, error) {
	if err != nil {
		return "", err
	}
	if valkeyResult, ok := result.(valkey.ValkeyResult); ok {
		return valkeyResult.ToString()
	}
	return fmt.Sprintf("%v", result), nil
}

// Int converts a valkey result to int (similar to redis.Int)
func Int(result interface{}, err error) (int, error) {
	if err != nil {
		return 0, err
	}
	if valkeyResult, ok := result.(valkey.ValkeyResult); ok {
		val, err := valkeyResult.ToInt64()
		return int(val), err
	}
	return 0, fmt.Errorf("cannot convert %T to int", result)
}

// Bool converts a valkey result to bool (similar to redis.Bool)
func Bool(result interface{}, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	if valkeyResult, ok := result.(valkey.ValkeyResult); ok {
		val, err := valkeyResult.ToInt64()
		return val != 0, err
	}
	return false, fmt.Errorf("cannot convert %T to bool", result)
}

// Float64 converts a valkey result to float64 (similar to redis.Float64)
func Float64(result interface{}, err error) (float64, error) {
	if err != nil {
		return 0, err
	}
	if valkeyResult, ok := result.(valkey.ValkeyResult); ok {
		return valkeyResult.ToFloat64()
	}
	return 0, fmt.Errorf("cannot convert %T to float64", result)
}

// Strings converts a valkey result to []string (similar to redis.Strings)
func Strings(result interface{}, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	if valkeyResult, ok := result.(valkey.ValkeyResult); ok {
		msgs, err := valkeyResult.ToArray()
		if err != nil {
			return nil, err
		}
		strings := make([]string, len(msgs))
		for i, msg := range msgs {
			s, err := msg.ToString()
			if err != nil {
				return nil, err
			}
			strings[i] = s
		}
		return strings, nil
	}
	return nil, fmt.Errorf("cannot convert %T to []string", result)
}

// StringMap converts a valkey result to map[string]string (similar to redis.StringMap)
func StringMap(result interface{}, err error) (map[string]string, error) {
	if err != nil {
		return nil, err
	}
	if valkeyResult, ok := result.(valkey.ValkeyResult); ok {
		msgMap, err := valkeyResult.ToMap()
		if err != nil {
			return nil, err
		}
		stringMap := make(map[string]string, len(msgMap))
		for k, v := range msgMap {
			s, err := v.ToString()
			if err != nil {
				return nil, err
			}
			stringMap[k] = s
		}
		return stringMap, nil
	}
	return nil, fmt.Errorf("cannot convert %T to map[string]string", result)
}

// Values converts a valkey result to []interface{} (similar to redis.Values)
func Values(result interface{}, err error) ([]interface{}, error) {
	if err != nil {
		return nil, err
	}
	if valkeyResult, ok := result.(valkey.ValkeyResult); ok {
		// Convert array to []interface{}
		msgs, err := valkeyResult.ToArray()
		if err != nil {
			return nil, err
		}
		result := make([]interface{}, len(msgs))
		for i, msg := range msgs {
			// Convert to []byte for compatibility with redigo
			s, err := msg.ToString()
			if err != nil {
				return nil, err
			}
			result[i] = []byte(s)
		}
		return result, nil
	}
	return nil, fmt.Errorf("cannot convert %T to []interface{}", result)
}