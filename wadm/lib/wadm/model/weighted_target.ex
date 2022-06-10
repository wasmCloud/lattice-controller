defmodule Wadm.Model.WeightedTarget do
  alias __MODULE__

  @type t :: %WeightedTarget{
          name: String.t(),
          requirements: Map.t(),
          weight: integer
        }

  @enforce_keys [:name]
  @derive Jason.Encoder
  defstruct [:name, :requirements, :weight]
end
