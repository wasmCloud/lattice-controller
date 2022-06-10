defmodule Wadm.Model.ActorComponent do
  alias __MODULE__
  alias Wadm.Model.{SpreadScaler, LinkDefinition}

  @derive Jason.Encoder
  defstruct [:name, :image, traits: []]

  @type trait :: SpreadScaler.t() | LinkDefinition.t()

  @type t :: %ActorComponent{
          name: String.t(),
          image: String.t(),
          traits: [trait()]
        }

  @spec new(String.t(), String.t(), [trait()]) :: ActorComponent.t()
  def new(name, image, traits) do
    %ActorComponent{
      name: name,
      image: image,
      traits: traits
    }
  end
end
