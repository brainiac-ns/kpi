class Job1:
    def __init__(self) -> None:
        pass

    def __call__(self, mess) -> str:
        """
        Does concatenation of input message with !.

        Args:
            mess (str): Input message
        
        Returns:
            str: Concatenated string
        """
        print(mess)
        return mess + "!"