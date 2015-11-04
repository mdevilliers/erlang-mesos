-module (recordio).

-export ([parse/1]).

-spec parse(binary()) -> [binary()].

parse(<<>>)-> [];
parse(Bin) when is_binary(Bin) ->
    [Len, Bin2] = binary:split(Bin, <<"\n">>), % split on 0xA
    case split_on_length(Bin2, binary_to_integer(Len)) of
        {Protobuf,Rest} ->
            [Protobuf| parse(Rest)]
    end.

% private
-spec split_on_length(binary(),non_neg_integer()) -> {binary(),binary()}.

split_on_length(Bin, Length) ->
    <<A:Length/binary, Rest/binary>> = Bin,
    {A,Rest}.
