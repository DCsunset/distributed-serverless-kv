// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.23.0
// 	protoc        v3.12.3
// source: db.proto

package db

import (
	context "context"
	proto "github.com/golang/protobuf/proto"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// This is a compile-time assertion that a sufficiently up-to-date version
// of the legacy proto package is being used.
const _ = proto.ProtoPackageIsVersion4

type GetRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	SessionId int64 `protobuf:"varint,1,opt,name=SessionId,proto3" json:"SessionId,omitempty"`
	// Empty to get all keys at location
	Keys       []string `protobuf:"bytes,2,rep,name=Keys,proto3" json:"Keys,omitempty"`
	Loc        int64    `protobuf:"varint,3,opt,name=Loc,proto3" json:"Loc,omitempty"`
	VirtualLoc int64    `protobuf:"varint,4,opt,name=VirtualLoc,proto3" json:"VirtualLoc,omitempty"`
}

func (x *GetRequest) Reset() {
	*x = GetRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_db_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetRequest) ProtoMessage() {}

func (x *GetRequest) ProtoReflect() protoreflect.Message {
	mi := &file_db_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetRequest.ProtoReflect.Descriptor instead.
func (*GetRequest) Descriptor() ([]byte, []int) {
	return file_db_proto_rawDescGZIP(), []int{0}
}

func (x *GetRequest) GetSessionId() int64 {
	if x != nil {
		return x.SessionId
	}
	return 0
}

func (x *GetRequest) GetKeys() []string {
	if x != nil {
		return x.Keys
	}
	return nil
}

func (x *GetRequest) GetLoc() int64 {
	if x != nil {
		return x.Loc
	}
	return 0
}

func (x *GetRequest) GetVirtualLoc() int64 {
	if x != nil {
		return x.VirtualLoc
	}
	return 0
}

type GetResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Key-value pairs
	Data map[string]string `protobuf:"bytes,1,rep,name=Data,proto3" json:"Data,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (x *GetResponse) Reset() {
	*x = GetResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_db_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetResponse) ProtoMessage() {}

func (x *GetResponse) ProtoReflect() protoreflect.Message {
	mi := &file_db_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetResponse.ProtoReflect.Descriptor instead.
func (*GetResponse) Descriptor() ([]byte, []int) {
	return file_db_proto_rawDescGZIP(), []int{1}
}

func (x *GetResponse) GetData() map[string]string {
	if x != nil {
		return x.Data
	}
	return nil
}

type SetRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	SessionId int64 `protobuf:"varint,1,opt,name=SessionId,proto3" json:"SessionId,omitempty"`
	// Key-value pairs
	Data map[string]string `protobuf:"bytes,2,rep,name=Data,proto3" json:"Data,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	// Its virtual location
	VirtualLoc int64 `protobuf:"varint,3,opt,name=VirtualLoc,proto3" json:"VirtualLoc,omitempty"`
	// Dependent location
	Dep int64 `protobuf:"varint,4,opt,name=Dep,proto3" json:"Dep,omitempty"`
	// Dependent virtual location
	VirtualDep int64 `protobuf:"varint,5,opt,name=VirtualDep,proto3" json:"VirtualDep,omitempty"`
}

func (x *SetRequest) Reset() {
	*x = SetRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_db_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SetRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SetRequest) ProtoMessage() {}

func (x *SetRequest) ProtoReflect() protoreflect.Message {
	mi := &file_db_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SetRequest.ProtoReflect.Descriptor instead.
func (*SetRequest) Descriptor() ([]byte, []int) {
	return file_db_proto_rawDescGZIP(), []int{2}
}

func (x *SetRequest) GetSessionId() int64 {
	if x != nil {
		return x.SessionId
	}
	return 0
}

func (x *SetRequest) GetData() map[string]string {
	if x != nil {
		return x.Data
	}
	return nil
}

func (x *SetRequest) GetVirtualLoc() int64 {
	if x != nil {
		return x.VirtualLoc
	}
	return 0
}

func (x *SetRequest) GetDep() int64 {
	if x != nil {
		return x.Dep
	}
	return 0
}

func (x *SetRequest) GetVirtualDep() int64 {
	if x != nil {
		return x.VirtualDep
	}
	return 0
}

type SetResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Location of the changes
	Loc int64 `protobuf:"varint,1,opt,name=Loc,proto3" json:"Loc,omitempty"`
}

func (x *SetResponse) Reset() {
	*x = SetResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_db_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SetResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SetResponse) ProtoMessage() {}

func (x *SetResponse) ProtoReflect() protoreflect.Message {
	mi := &file_db_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SetResponse.ProtoReflect.Descriptor instead.
func (*SetResponse) Descriptor() ([]byte, []int) {
	return file_db_proto_rawDescGZIP(), []int{3}
}

func (x *SetResponse) GetLoc() int64 {
	if x != nil {
		return x.Loc
	}
	return 0
}

var File_db_proto protoreflect.FileDescriptor

var file_db_proto_rawDesc = []byte{
	0x0a, 0x08, 0x64, 0x62, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x02, 0x64, 0x62, 0x22, 0x70,
	0x0a, 0x0a, 0x47, 0x65, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x1c, 0x0a, 0x09,
	0x53, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x49, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52,
	0x09, 0x53, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e, 0x49, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x4b, 0x65,
	0x79, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x09, 0x52, 0x04, 0x4b, 0x65, 0x79, 0x73, 0x12, 0x10,
	0x0a, 0x03, 0x4c, 0x6f, 0x63, 0x18, 0x03, 0x20, 0x01, 0x28, 0x03, 0x52, 0x03, 0x4c, 0x6f, 0x63,
	0x12, 0x1e, 0x0a, 0x0a, 0x56, 0x69, 0x72, 0x74, 0x75, 0x61, 0x6c, 0x4c, 0x6f, 0x63, 0x18, 0x04,
	0x20, 0x01, 0x28, 0x03, 0x52, 0x0a, 0x56, 0x69, 0x72, 0x74, 0x75, 0x61, 0x6c, 0x4c, 0x6f, 0x63,
	0x22, 0x75, 0x0a, 0x0b, 0x47, 0x65, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12,
	0x2d, 0x0a, 0x04, 0x44, 0x61, 0x74, 0x61, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x19, 0x2e,
	0x64, 0x62, 0x2e, 0x47, 0x65, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x2e, 0x44,
	0x61, 0x74, 0x61, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x04, 0x44, 0x61, 0x74, 0x61, 0x1a, 0x37,
	0x0a, 0x09, 0x44, 0x61, 0x74, 0x61, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b,
	0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a,
	0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61,
	0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x22, 0xe3, 0x01, 0x0a, 0x0a, 0x53, 0x65, 0x74, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x1c, 0x0a, 0x09, 0x53, 0x65, 0x73, 0x73, 0x69, 0x6f,
	0x6e, 0x49, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x09, 0x53, 0x65, 0x73, 0x73, 0x69,
	0x6f, 0x6e, 0x49, 0x64, 0x12, 0x2c, 0x0a, 0x04, 0x44, 0x61, 0x74, 0x61, 0x18, 0x02, 0x20, 0x03,
	0x28, 0x0b, 0x32, 0x18, 0x2e, 0x64, 0x62, 0x2e, 0x53, 0x65, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x2e, 0x44, 0x61, 0x74, 0x61, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x52, 0x04, 0x44, 0x61,
	0x74, 0x61, 0x12, 0x1e, 0x0a, 0x0a, 0x56, 0x69, 0x72, 0x74, 0x75, 0x61, 0x6c, 0x4c, 0x6f, 0x63,
	0x18, 0x03, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0a, 0x56, 0x69, 0x72, 0x74, 0x75, 0x61, 0x6c, 0x4c,
	0x6f, 0x63, 0x12, 0x10, 0x0a, 0x03, 0x44, 0x65, 0x70, 0x18, 0x04, 0x20, 0x01, 0x28, 0x03, 0x52,
	0x03, 0x44, 0x65, 0x70, 0x12, 0x1e, 0x0a, 0x0a, 0x56, 0x69, 0x72, 0x74, 0x75, 0x61, 0x6c, 0x44,
	0x65, 0x70, 0x18, 0x05, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0a, 0x56, 0x69, 0x72, 0x74, 0x75, 0x61,
	0x6c, 0x44, 0x65, 0x70, 0x1a, 0x37, 0x0a, 0x09, 0x44, 0x61, 0x74, 0x61, 0x45, 0x6e, 0x74, 0x72,
	0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03,
	0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x22, 0x1f, 0x0a,
	0x0b, 0x53, 0x65, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x10, 0x0a, 0x03,
	0x4c, 0x6f, 0x63, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x03, 0x4c, 0x6f, 0x63, 0x32, 0x5f,
	0x0a, 0x09, 0x44, 0x62, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x28, 0x0a, 0x03, 0x47,
	0x65, 0x74, 0x12, 0x0e, 0x2e, 0x64, 0x62, 0x2e, 0x47, 0x65, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x1a, 0x0f, 0x2e, 0x64, 0x62, 0x2e, 0x47, 0x65, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x22, 0x00, 0x12, 0x28, 0x0a, 0x03, 0x53, 0x65, 0x74, 0x12, 0x0e, 0x2e, 0x64,
	0x62, 0x2e, 0x53, 0x65, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x0f, 0x2e, 0x64,
	0x62, 0x2e, 0x53, 0x65, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x62,
	0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_db_proto_rawDescOnce sync.Once
	file_db_proto_rawDescData = file_db_proto_rawDesc
)

func file_db_proto_rawDescGZIP() []byte {
	file_db_proto_rawDescOnce.Do(func() {
		file_db_proto_rawDescData = protoimpl.X.CompressGZIP(file_db_proto_rawDescData)
	})
	return file_db_proto_rawDescData
}

var file_db_proto_msgTypes = make([]protoimpl.MessageInfo, 6)
var file_db_proto_goTypes = []interface{}{
	(*GetRequest)(nil),  // 0: db.GetRequest
	(*GetResponse)(nil), // 1: db.GetResponse
	(*SetRequest)(nil),  // 2: db.SetRequest
	(*SetResponse)(nil), // 3: db.SetResponse
	nil,                 // 4: db.GetResponse.DataEntry
	nil,                 // 5: db.SetRequest.DataEntry
}
var file_db_proto_depIdxs = []int32{
	4, // 0: db.GetResponse.Data:type_name -> db.GetResponse.DataEntry
	5, // 1: db.SetRequest.Data:type_name -> db.SetRequest.DataEntry
	0, // 2: db.DbService.Get:input_type -> db.GetRequest
	2, // 3: db.DbService.Set:input_type -> db.SetRequest
	1, // 4: db.DbService.Get:output_type -> db.GetResponse
	3, // 5: db.DbService.Set:output_type -> db.SetResponse
	4, // [4:6] is the sub-list for method output_type
	2, // [2:4] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_db_proto_init() }
func file_db_proto_init() {
	if File_db_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_db_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_db_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_db_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SetRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_db_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SetResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_db_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   6,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_db_proto_goTypes,
		DependencyIndexes: file_db_proto_depIdxs,
		MessageInfos:      file_db_proto_msgTypes,
	}.Build()
	File_db_proto = out.File
	file_db_proto_rawDesc = nil
	file_db_proto_goTypes = nil
	file_db_proto_depIdxs = nil
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConnInterface

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion6

// DbServiceClient is the client API for DbService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type DbServiceClient interface {
	Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error)
	Set(ctx context.Context, in *SetRequest, opts ...grpc.CallOption) (*SetResponse, error)
}

type dbServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewDbServiceClient(cc grpc.ClientConnInterface) DbServiceClient {
	return &dbServiceClient{cc}
}

func (c *dbServiceClient) Get(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error) {
	out := new(GetResponse)
	err := c.cc.Invoke(ctx, "/db.DbService/Get", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *dbServiceClient) Set(ctx context.Context, in *SetRequest, opts ...grpc.CallOption) (*SetResponse, error) {
	out := new(SetResponse)
	err := c.cc.Invoke(ctx, "/db.DbService/Set", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// DbServiceServer is the server API for DbService service.
type DbServiceServer interface {
	Get(context.Context, *GetRequest) (*GetResponse, error)
	Set(context.Context, *SetRequest) (*SetResponse, error)
}

// UnimplementedDbServiceServer can be embedded to have forward compatible implementations.
type UnimplementedDbServiceServer struct {
}

func (*UnimplementedDbServiceServer) Get(context.Context, *GetRequest) (*GetResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Get not implemented")
}
func (*UnimplementedDbServiceServer) Set(context.Context, *SetRequest) (*SetResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Set not implemented")
}

func RegisterDbServiceServer(s *grpc.Server, srv DbServiceServer) {
	s.RegisterService(&_DbService_serviceDesc, srv)
}

func _DbService_Get_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DbServiceServer).Get(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/db.DbService/Get",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DbServiceServer).Get(ctx, req.(*GetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _DbService_Set_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(DbServiceServer).Set(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/db.DbService/Set",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(DbServiceServer).Set(ctx, req.(*SetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _DbService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "db.DbService",
	HandlerType: (*DbServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Get",
			Handler:    _DbService_Get_Handler,
		},
		{
			MethodName: "Set",
			Handler:    _DbService_Set_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "db.proto",
}
