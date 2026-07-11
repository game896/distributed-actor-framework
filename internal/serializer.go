package internal

import (
	"fmt"
	"sync"

	"github.com/anomalyco/distributed-actor-framework/actor"
	pb "github.com/anomalyco/distributed-actor-framework/remote/proto/generated"
	"google.golang.org/protobuf/proto"
)

var (
	typeRegistry sync.Map
)

func RegisterType(name string, factory func() proto.Message) {
	typeRegistry.Store(name, factory)
}

func MarshalMessage(msg *actor.Message) (*pb.MessageEnvelope, error) {
	env := &pb.MessageEnvelope{
		Sender: &pb.PID{
			Id:      msg.Sender.ID,
			Address: msg.Sender.Address,
		},
		Receiver: &pb.PID{
			Id:      msg.Receiver.ID,
			Address: msg.Receiver.Address,
		},
		MessageType: msg.Type,
	}

	if msg.Payload == nil {
		return env, nil
	}

	pbMsg, ok := msg.Payload.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("serializer: payload type %T does not implement proto.Message", msg.Payload)
	}

	bytes, err := proto.Marshal(pbMsg)
	if err != nil {
		return nil, fmt.Errorf("serializer: failed to marshal payload: %w", err)
	}

	name := string(pbMsg.ProtoReflect().Descriptor().FullName())
	env.PayloadType = name
	env.PayloadBytes = bytes

	return env, nil
}

func UnmarshalMessage(env *pb.MessageEnvelope) (*actor.Message, error) {
	msg := &actor.Message{
		Sender: actor.PID{
			ID:      env.Sender.GetId(),
			Address: env.Sender.GetAddress(),
		},
		Receiver: actor.PID{
			ID:      env.Receiver.GetId(),
			Address: env.Receiver.GetAddress(),
		},
		Type: env.GetMessageType(),
	}

	if env.PayloadBytes == nil {
		return msg, nil
	}

	typeName := env.GetPayloadType()
	if typeName == "" {
		return msg, nil
	}

	factoryVal, ok := typeRegistry.Load(typeName)
	if !ok {
		return nil, fmt.Errorf("serializer: unknown payload type %s (register with internal.RegisterType)", typeName)
	}

	factory, ok := factoryVal.(func() proto.Message)
	if !ok {
		return nil, fmt.Errorf("serializer: invalid factory for type %s", typeName)
	}

	instance := factory()
	if err := proto.Unmarshal(env.PayloadBytes, instance); err != nil {
		return nil, fmt.Errorf("serializer: failed to unmarshal payload: %w", err)
	}

	msg.Payload = instance
	return msg, nil
}
