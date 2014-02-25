#ifndef __MESOS_C_UTILS_HPP__
#define __MESOS_C_UTILS_HPP__

#include "mesos/mesos.pb.h"
#include "erl_nif.h"

template <class T> 
ERL_NIF_TERM pb_obj_to_binary(ErlNifEnv *env, const T& obj)  {
    ErlNifBinary res;
    enif_alloc_binary(obj.ByteSize(), &res); // Review : do I need to dealloc this?
    obj.SerializeToArray(res.data, res.size);
    return enif_make_binary(env, &res);
}

template<typename T> inline bool deserialize(T& ret, void* data, size_t size)
  {
    if (!ret.ParseFromArray(data, size)) {
      printf("Deserialization failed\n");
      return false;
    }
    return true;
}

template<typename T> inline bool deserialize(T& ret, ErlNifBinary* obj) {
  if (obj == NULL) {
    return false;
  }
  return deserialize<T>(ret, obj->data, obj->size);
}

template<typename T> inline bool deserialize(
      std::vector<T>& ret,
      BinaryNifArray* request)
  {
    for(int i = 0; i < request->length; i++)
    {
      T obj;
      if(!deserialize<T>(obj, &request->obj[i])){return false;}
      ret.push_back(obj);
    }
    return true;
  }

#endif
  